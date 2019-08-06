package pdEventTransportSubsystem;

import java.io.*;
import java.net.*;

import bamboo.bbPrinter;

import pdEventTransportSubsystem.events.*;
import pdObjectSubsystem.*;
import pdKernel.*;


/**
 * IP Multicast event transporter
 *
 * @author BARS Team
 */
public class pdEventTransporterIPMulticastImpl extends pdActiveSystemObject
      implements pdEventTransporter, Runnable
  {
    /** IP multicast address to use */
    protected String multicastAddress = null;

    /** IP multicast port to use */
    protected int port = -1;

    /** IP multicast port to use */
    transient protected MulticastSocket socket = null;

    /** group to belong to */
    transient protected InetAddress group = null;

    /** receiving buffer */
    transient private byte[] buffer = new byte[ 65536 ];

    /** The thread for this runnable object */
    transient protected Thread thisThread = null;

    /** Has the thread been started? */
    boolean threadStarted = false;

    /** The object that gets the events we receive. */
    transient protected pdEventAccepter eventAccepter = null;

    /** Size of event history array. */
    protected int EVENTIDHISTORYSIZE = 500;

    /**
     * Rolling array of event IDs sent; if we get an event back that
     * we've sent, ignore it!
     */
    transient protected long[] sentEventIDs = new long[ EVENTIDHISTORYSIZE ];

    /** Index in event array of the most recently sent event. */
    transient protected int lastSentEventIndex = -1;

    /** currently collecting statistics? */
    transient protected boolean isCollectingStats = false;

    /** statistics we might be collecting */
    transient pdEventTransporterStatistics eventStats = 
      new pdEventTransporterStatistics( "IPMulticast" );


    /** Make a new IPMulticast transporter using this multicast address and port. */
    public pdEventTransporterIPMulticastImpl( String multicastAddress, int port )
    throws Exception
      {
        this.multicastAddress = multicastAddress;
        this.port = port;

        for ( int i = 0; i < EVENTIDHISTORYSIZE; i++ )
          sentEventIDs[ i ] = -1;
      }


    /**
     * Initialize a remote copy of this transporter.
     */
    public void initializeRemoteCopy() throws Exception
      {
        lastSentEventIndex = -1;
        for ( int i = 0; i < EVENTIDHISTORYSIZE; i++ )
          sentEventIDs[ i ] = -1;
        group = null;
        socket = null;
        this.status = pdActiveObject.STOPPED;
      }


    /** Check if this event was echoed back to us from IP multicast. */
    protected boolean wasEventEchoedBack( pdBasicEvent event )
    {
      long eventID = event.getEventID();

      // See if this event is one we just sent! If so, ignore it.
      for ( int i = 0; i < EVENTIDHISTORYSIZE; i++ )
        {
          if ( sentEventIDs[ i ] == eventID )
            {
              //bbPrinter.bbDebug("Event was echoed back...\n");
              return true;
            }
        }

      return false;
    }


    ////////////////////////////////////////////////////////////
    //                                                        //
    //  pdActiveSystemObject abstract method implementations  //
    //                                                        //
    ////////////////////////////////////////////////////////////

    /** Start this thread. */
    protected boolean _start()
    {
      if ( eventAccepter == null )
        {
          bbPrinter.bbError( "Cannot start transporter: event accepter is null.\n" );
          return false;
        }

      // Create the socket if necessary.
      if ( socket == null )
        {
          try
            {
              group = InetAddress.getByName( multicastAddress );
              socket = new MulticastSocket( port );
              //socket.setTTL((new Byte("99")).byteValue());
              socket.setSoTimeout( 20 );              
              socket.joinGroup( group );
            }
          catch ( Exception e )
            {
              bbPrinter.bbError( "pdEventTransporterIPMulticastImpl: could not start: " + e.getMessage() + "\n" );
              return false;
            }
        }

      thisThread = new Thread( this );
      thisThread.start();
      threadStarted = true;
      return true;
    }


    /** Stop this thread. */
    protected boolean _stop()
    {
      thisThread.stop();
      threadStarted = false;

      if ( socket != null )
        {
          try
            {
              // Shut down socket
              socket.leaveGroup( group );
            }
          catch ( Exception e )
            {
              bbPrinter.bbError( "pdEventTransporterIPMulticastImpl: could not stop: " + e.getMessage() + "\n" );
              return false;
            }
          socket = null;
        }

      return true;
    }

    /** Suspend this thread. */
    protected boolean _suspend()
    {
      return false;
    }


    /** Resume this thread. */
    protected boolean _resume()
    {
      return false;
    }


    ///////////////////////////////////////
    //                                   //
    //  Runnable method implementations  //
    //                                   //
    ///////////////////////////////////////

    /**
     * The main thread: try to receive an event from the socket. If it's
     * a valid event, send it to the event-accepting object.
     */
    public void run()
    {
      pdBasicEvent event = null;

      while ( true )
        {
          event = null;

          try
            {
              DatagramPacket eventDatagram = new DatagramPacket( buffer, buffer.length );
              bbPrinter.bbDebug( "Transporter trying to receive event... \n" );
              socket.receive( eventDatagram );
              bbPrinter.bbDebug( "Transporter received event maybe \n" );
              byte[] eventBuffer = eventDatagram.getData();
              ByteArrayInputStream inByteStream = new ByteArrayInputStream( eventBuffer );
              ObjectInputStream inObjectStream = new ObjectInputStream( inByteStream );
              event = ( pdBasicEvent ) inObjectStream.readObject();
              inObjectStream.close();
            }
          catch ( java.net.SocketTimeoutException jns )
            {
              // do nothing
            }
          catch ( Exception e )
            {
              e.printStackTrace();
            }
      
          if ( event == null )
            continue; 
 
          if ( wasEventEchoedBack( event ) )
            continue;

          bbPrinter.bbDebug( "Transporter received event of type " + event.getClass().getName() + "\n" );

          bbPrinter.bbDebug( "IPMulticast: event not from me!\n" );

          bbPrinter.bbDebug( "IPMulticast: eventAccepter is " + ( ( pdSystemObject ) eventAccepter ).getName() + "\n" );


          if ( eventAccepter != null )
            eventAccepter.acceptEvent( event );


          if ( isCollectingStats )
            eventStats.countEventReceived( event );


          // Sleep now or else this thread will dominate and slow everything down A LOT.
          try
            {
              Thread.sleep( 20 );
            }
          catch ( Exception e )
            {
              e.printStackTrace();
            }
        }
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


    /** Send an event to the network. */
    public void sendEvent( pdBasicEvent event )
    {
      // Don't send null events...
      if ( event == null )
        return ;

      // Ignore this event if this object hasn't been started yet.
      if ( socket == null )
        return ;

      bbPrinter.bbDebug( "IPMulticast Transporter sending " + event.getClass().getName() + ": " + event.toString() + "\n" );


      // Add the event's ID to the array of recently-sent IDs.
      lastSentEventIndex = ( lastSentEventIndex + 1 ) % EVENTIDHISTORYSIZE;
      sentEventIDs[ lastSentEventIndex ] = event.getEventID();

      try
        {
          ByteArrayOutputStream outByteStream = new ByteArrayOutputStream();
          ObjectOutputStream outObjectStream = new ObjectOutputStream( outByteStream );
          outObjectStream.writeObject( event );
          outObjectStream.flush();
          outObjectStream.close();
          byte[] buffer = outByteStream.toByteArray();
          DatagramPacket eventDatagram = new DatagramPacket( buffer, buffer.length, group, port );
          socket.send( eventDatagram );
        }
      catch ( java.net.NoRouteToHostException e1 )
        {
          bbPrinter.bbDebug("there is not route to host to dispatch event");
          //e1.printStackTrace();
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




