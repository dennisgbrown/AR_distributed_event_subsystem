package pdEventTransportSubsystem;

import java.io.*;
import java.net.*;
import java.util.*;

import inria.net.lrmp.*;

import bamboo.bbPrinter;

import pdEventTransportSubsystem.events.*;
import pdKernel.*;


/**
 * LRMP-based event transporter. LRMP (lightweight reliable
 * multicast protocol) was created at INRIA in France and is
 * free for both research and commercial use (see http://webcanal.inria.fr/lrmp/).
 * <P>
 * Large chunks here are taken from an LRMP example program
 * currently at http://webcanal.inria.fr/lrmp/lrmp_example.html
 *
 * @author BARS Team
 */
public class pdEventTransporterLRMPImpl extends pdActiveSystemObject
      implements pdEventTransporter, LrmpEventHandler
  {
    /** Multicast address to use */
    protected String multicastAddress = null;

    /** Multicast port to use */
    protected int port = -1;

    /** The LRMP instance */
    transient protected Lrmp lrmp = null;

    /** Has LRMP been started? */
    transient protected boolean lrmpStarted = false;

    /** Size of a packet of data, hard-coded for your inconvenience */
    int PACKET_DATA_LENGTH = 1024;

    /** The maximum size we expect of a serialized event, also hard-coded for your inconvenience */
    int MAX_EVENT_SIZE = 65536;

    /** The event accepter. */
    transient protected pdEventAccepter eventAccepter = null;

    /** profile info for LRMP */
    transient protected LrmpProfile profile = null;

    /**
     * A table of partially-received events, which we collect until
     * the whole event is received and can be un-serialized and used. 
     */
    transient protected Hashtable senderToPartialEventTable = new Hashtable();

    /** currently collecting statistics? */
    transient protected boolean isCollectingStats = false;

    /** statistics we might be collecting */
    transient pdEventTransporterStatistics eventStats = 
      new pdEventTransporterStatistics( "LRMP" );


    /** Create a new LRMP transporter for this multicast address and port. */
    public pdEventTransporterLRMPImpl( String multicastAddress, int port )
    throws Exception
      {
        this.multicastAddress = multicastAddress;
        this.port = port;
        createLRMPProfile();
      }


    /**
     * Initialize a remote copy of this transporter.
     */
    public void initializeRemoteCopy() throws Exception
      {
        this.status = pdActiveObject.STOPPED;
        lrmpStarted = false;
        senderToPartialEventTable = new Hashtable();
        createLRMPProfile();
        lrmp = null;
      }


    /** Create LRMP profile info */
    private void createLRMPProfile()
    {
      // the LRMP configuration is set through profile. All config parameters
      // given here are the default values.
      profile = new LrmpProfile();

      // set the handler for processing the received packets and events
      profile.setEventHandler( this );

      // the reliability
      profile.reliability = LrmpProfile.LimitedLoss;

      // the sequencing
      profile.ordered = true;

      // the flow control
      profile.throughput = LrmpProfile.BestEffort;

      // the data rate in kbits/sec if the throughput is set to AdaptedThroughput
      profile.minRate = 8;
      profile.maxRate = 64;

      // the buffer space in kilo bytes
      profile.sendWindowSize = 1024;
      profile.rcvWindowSize = 1024;
      
      // the receiver report mechanism
      profile.rcvReportSelection = LrmpProfile.RandomReceiverReport;
    }


    ////////////////////////////////////////////////////////////
    //                                                        //
    //  pdActiveSystemObject abstract method implementations  //
    //                                                        //
    ////////////////////////////////////////////////////////////


    /** Start LRMP. */
    protected boolean _start()
    {
      if ( eventAccepter == null )
        {
          bbPrinter.bbError( "Cannot start transporter: event accepter is null.\n" );
          return false;
        }

      // create an LRMP object if necessary
      if ( lrmp == null )
        {
          try
            {
              // Uses fixed TTL--change?
              lrmp = new Lrmp( multicastAddress, port, 20, profile );
              bbPrinter.bbDebug( "LRMP started on " + multicastAddress + ":" + port + "\n" );
            }
          catch ( Exception e )
            {
              bbPrinter.bbError( "pdEventTransporterLRMPImpl: could not start: " + e.getMessage() + "\n" );
              return false;
            }
        }

      // start the LRMP object. From this time, packets will be received
      lrmp.start();
      lrmpStarted = true;
      return true;
    }


    /** Stop LRMP. */
    protected boolean _stop()
    {
      // Shut down lrmp
      if ( lrmpStarted )
        lrmp.stop();
      lrmpStarted = false;
      lrmp = null;

      return true;
    }


    /** Suspend nothing. */
    protected boolean _suspend()
    {
      return false;
    }


    /** Resume nothing. */
    protected boolean _resume()
    {
      return false;
    }


    ///////////////////////////////////////////////
    //                                           //
    //  LrmpEventHandler method implementations  //
    //                                           //
    ///////////////////////////////////////////////

    /**
     * This method is defined in LrmpEventHandler. Every time an in-sequence
     * data packet is received, this method is called.
     * It checks to see that this packet represents an event or part of
     * an event. If it's part of an event, that part is stored away. If
     * it's a whole event OR the LAST part of a multi-packet event, the event is
     * un-serialized into an event object and sent to the event accepter.
     */
    public void processData( LrmpPacket pack )
    {
      if ( !pack.isReliable() )
        {
          // only use reliable transport here...
          return ;
        }

      pdBasicEvent event = null;

      // Get the part number of this part of an event and the sender.
      int partNumber = ( new Byte( ( pack.getDataBuffer() ) [ pack.getOffset() ] ).intValue() );
      int numParts = ( new Byte( ( pack.getDataBuffer() ) [ ( pack.getOffset() + 1 ) ] ).intValue() );
      LrmpEntity sender = pack.getSource();
      bbPrinter.bbDebug( "pdEventTransporterLRMPImpl:Got event part " + partNumber + "/" + numParts + " of size " + ( pack.getDataLength() - 2 ) );

      // If this is a one-part event just parse it into an event.
      if ( ( partNumber == numParts ) && ( numParts == 1 ) )
        {
          try
            {
              ByteArrayInputStream inByteStream = new ByteArrayInputStream( pack.getDataBuffer(), pack.getOffset() + 2, pack.getDataLength() - 2 );
              ObjectInputStream inObjectStream = new ObjectInputStream( inByteStream );
              event = ( pdBasicEvent ) inObjectStream.readObject();
              inObjectStream.close();
            }
          catch ( Exception e )
            {
              e.printStackTrace();
            }
        }

      // Otherwise, this part is one of many...
      else
        {
          // If it's the last part, construct the event.
          if ( partNumber == numParts )
            {
              if ( senderToPartialEventTable.containsKey( sender ) )
                {
                  byte[] buffer = ( byte[] ) ( senderToPartialEventTable.get( sender ) );
                  System.arraycopy( pack.getDataBuffer(), pack.getOffset() + 2,
                                    buffer, ( ( partNumber - 1 ) * PACKET_DATA_LENGTH ),
                                    ( pack.getDataLength() - 2 ) );

                  try
                    {
                      ByteArrayInputStream inByteStream = new ByteArrayInputStream( buffer );
                      ObjectInputStream inObjectStream = new ObjectInputStream( inByteStream );
                      event = ( pdBasicEvent ) inObjectStream.readObject();
                      inObjectStream.close();
                    }
                  catch ( Exception e )
                    {
                      e.printStackTrace();
                    }

                  senderToPartialEventTable.remove( sender );
                }
              else
                {
                  bbPrinter.bbDebug( "pdEventTransporterLRMPImpl:LRMP ERROR: Got event part " + partNumber + "/" + numParts + " from sender " + sender + " but missing other parts!!!\n" );
                }
            }

          // If it's the first part, create the table entry.
          else if ( partNumber == 1 )
            {
              if ( !( senderToPartialEventTable.containsKey( sender ) ) )
                {
                  byte[] buffer = new byte[ MAX_EVENT_SIZE ];
                  System.arraycopy( pack.getDataBuffer(), pack.getOffset() + 2,
                                    buffer, 0,
                                    PACKET_DATA_LENGTH );
                  senderToPartialEventTable.put( sender, buffer );
                }
              else
                {
                  bbPrinter.bbDebug( "LRMP ERROR: Got event part " + partNumber + "/" + numParts + " from sender " + sender + " but already have other parts!!!\n" );
                }
            }

          // Else just add it to the existing partial event
          // for this sender.
          else
            {
              if ( senderToPartialEventTable.containsKey( sender ) )
                {
                  byte[] buffer = ( byte[] ) ( senderToPartialEventTable.get( sender ) );
                  System.arraycopy( pack.getDataBuffer(), pack.getOffset() + 2,
                                    buffer, ( ( partNumber - 1 ) * PACKET_DATA_LENGTH ),
                                    PACKET_DATA_LENGTH );
                }
              else
                {
                  bbPrinter.bbDebug( "LRMP ERROR: Got event part " + partNumber + "/" + numParts + " from sender " + sender + " but missing other parts!!!\n" );
                }
            }
        }

      // If we got an event somehow out of the mess above, send it up...
      if ( event != null )
        {
          bbPrinter.bbDebug( "LRMP Transporter receiving " + event.getClass().getName() + ": " + event.getEventID() + "\n" );
          
          if ( eventAccepter != null )
            eventAccepter.acceptEvent( event );

          if ( isCollectingStats )
            eventStats.countEventReceived( event );
        }
    }


    /** This method is defined in LrmpEventHandler but isn't used for anything. */
    public void processEvent( int event, Object obj )
    {
      return ;
    }


    /////////////////////////////////////////////////
    //                                             //
    //  pdEventTransporter method implementations  //
    //                                             //
    /////////////////////////////////////////////////

    /** Set the event accepting object. */
    public void setEventAccepter( pdEventAccepter eventAccepter )
    {
      this.eventAccepter = eventAccepter;
    }


    /** Get the event accepting object. */
    public pdEventAccepter getEventAccepter()
    {
      return this.eventAccepter;
    }


    /** Get the IP address this transporter is using. */
    public String getAddress()
    {
      return multicastAddress;
    }


    /** Get the IP port this transporter is using. */
    public int getPort()
    {
      return port;
    }


    /** 
     * Send an event to the network. Because LRMP has a low max packet size,
     * we may need to break the event up into many packets. In that case,
     * each packet contains a chunk of the event plus a couple of extra
     * bytes to represent the chunk number and the total number of chunks.
     */
    public void sendEvent( pdBasicEvent event )
    {
      // Don't send null events...
      if ( event == null )
        return ;

      // Ignore this event if this object hasn't been started yet.
      if ( lrmp == null )
        return ;

      bbPrinter.bbDebug( "LRMP Transporter sending " + event.getClass().getName() + ": " + event.getEventID() + "\n" );

      try
        {
          // Write the event into a byte buffer.
          ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
          ObjectOutputStream outObjectStream = new ObjectOutputStream( outByteStream );
          outObjectStream.writeObject( event );
          outObjectStream.flush();
          outObjectStream.close();
          byte[] eventBuffer = outByteStream.toByteArray();

          // Figure out how many packets we'll need.
          int totalNumParts = eventBuffer.length / PACKET_DATA_LENGTH;
          if ( ( eventBuffer.length % PACKET_DATA_LENGTH ) > 0 )
            totalNumParts++;
          int currPacket = 1;

          // Break the event into packet-sized chunks and send them.
          int eventBufferPosition = 0;
          int eventBufferBytesLeft = eventBuffer.length;

          bbPrinter.bbDebug( "pdEventTransporterLRMPImpl:Need to send event of size " + eventBuffer.length + "; will be " + totalNumParts + " packets." );

          while ( currPacket <= totalNumParts )
            {
              // Write the event's bytes into an LRMP packet.
              LrmpPacket pack = new LrmpPacket();
              byte[] packetBuffer = pack.getDataBuffer();
              int offset = pack.getOffset();
              packetBuffer[ offset ] = ( new Integer( currPacket ) ).byteValue();
              offset++;
              packetBuffer[ offset ] = ( new Integer( totalNumParts ) ).byteValue();
              offset++;

              if ( eventBufferBytesLeft >= PACKET_DATA_LENGTH )
                {
                  System.arraycopy( eventBuffer, eventBufferPosition,
                                    packetBuffer, offset,
                                    PACKET_DATA_LENGTH );
                  pack.setDataLength( PACKET_DATA_LENGTH + 2 ); // +1 for the part number bytes
                  //bbPrinter.bbDebug("pdEventTransporterLRMPImpl:Sent packet " + packetsLeft + " of size " + PACKET_DATA_LENGTH);
                }
              else
                {
                  System.arraycopy( eventBuffer, eventBufferPosition,
                                    packetBuffer, offset,
                                    eventBufferBytesLeft );
                  pack.setDataLength( eventBufferBytesLeft + 2 ); // +1 for the part number bytes
                  //bbPrinter.bbDebug("pdEventTransporterLRMPImpl:Sent packet " + packetsLeft + " of size " + eventBufferBytesLeft);
                }

              lrmp.send( pack );

              eventBufferBytesLeft -= PACKET_DATA_LENGTH;
              eventBufferPosition += PACKET_DATA_LENGTH;
              currPacket++;
            }
        }
      catch ( Exception e )
        {
          e.printStackTrace();
        }

      if ( isCollectingStats )
        eventStats.countEventSent( event );
    }


    /** Turn on statistics-collecting. */
    public void startCollectingStatistics()
    {
      this.isCollectingStats = true;
      eventStats.activate();
    }


    /** Turn off statistics-collecting. */
    public void stopCollectingStatistics()
    {
      this.isCollectingStats = false;
      eventStats.deactivate();
    }

    
    /** Return statistics for this transporter. */
    public pdEventTransporterStatistics getStatistics()
    {
      return eventStats;
    }
  }


