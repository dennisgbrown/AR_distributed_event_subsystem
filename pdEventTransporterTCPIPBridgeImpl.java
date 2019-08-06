package pdEventTransportSubsystem;

import java.io.*;
import java.net.*;
import java.util.*;

import bamboo.bbPrinter;

import pdEventTransportSubsystem.events.*;
import pdKernel.*;


/**
 * Send events to a serial broadcast (TCP/IP-based) server.
 * Read events from a the same serial broadcast (TCP/IP-based) server.
 * <P>
 * Nasty code: If we lose the connection, we try to regain it. However,
 * this goes on while the rest of the system is dispatching the event,
 * so at least one part of the system is stuck until we reconnect.
 * So there is a connection retry limit of 3; after that, it gives up
 * forever, so at least the local simulation can continue.
 * <P>
 * There really should be an event queue instead that holds events until
 * a connection is made so that the rest of the system doesn't need
 * to wait. Also, the queue should be checked to make sure it doesn't
 * grow too large while waiting for a connection that may never happen.
 * These improvements may come someday if we actually use this
 * transporter much.
 *
 * @author BARS Team
 */
public class pdEventTransporterTCPIPBridgeImpl extends pdActiveSystemObject
      implements pdEventTransporter, Runnable
  {
    /** The server address */
    protected String serverHostname = null;

    /** The server port */
    protected int serverPort = -1;

    /** Socket connection with server. */
    transient protected Socket socket = null;

    /** Read objects from the server. */
    transient protected ObjectInputStream objectsInput = null;

    /** Write objects to the server. */
    transient protected ObjectOutputStream objectsOutput = null;

    /** Thread for this runnable object. */
    transient protected Thread thisThread = null;

    /** Has this object's thread been started? */
    transient boolean threadStarted = false;

    /** Are we currently trying to connect to the server? */
    transient boolean tryingToConnect = false;

    /** Current try to connect to socket. */
    transient int currentTry = 0;

    /** Should we give up trying to connect to the socket? */
    transient boolean giveUp = false;

    /**
     * By default, we will receive events; can be set to false
     * for "send-only" applications (note: will still get object
     * creation/destruction events and transport events to maintain 
     * consistency).
     */
    boolean sendOnly = true;

    /** The event-accepting object. */
    protected pdEventAccepter eventAccepter = null;


    /** currently collecting statistics? */
    transient protected boolean isCollectingStats = false;

    /** statistics we might be collecting */
    transient pdEventTransporterStatistics eventStats = 
      new pdEventTransporterStatistics( "TCPIPBridge" );


    /** Create a new TCPIP transporter using this address and port. */
    public pdEventTransporterTCPIPBridgeImpl( String serverHostname, int serverPort )
    throws Exception
      {
        this.serverHostname = serverHostname;
        this.serverPort = serverPort;
      }


    /** Initialize a remote copy of this transporter. */
    public void initializeRemoteCopy() throws Exception
      {
        objectsInput = null;
        objectsOutput = null;
        thisThread = null;
        threadStarted = false;
        tryingToConnect = false;
        currentTry = 0;
        giveUp = false;
        this.status = pdActiveObject.STOPPED;
      }


    /** Get a connection to the server. */
    protected void getConnection()
    {
      // If we shouldn't try to connect, don't.
      if ( giveUp )
        return ;

      // If we're already trying to connect, don't try again (stupid
      // way of synchronizing code).
      if ( tryingToConnect )
        return ;
      tryingToConnect = true;

      // Should we give up yet?
      currentTry++;
      if ( currentTry >= 3 )
        giveUp = true;

      boolean notConnected = true;

      while ( notConnected )
        {
          try
            {
              // Open my outgoing event socket (incoming to server).
              bbPrinter.bbNotice( "EventTransporter: Opening socket to server " + serverHostname + ":" + serverPort + "\n" );
              socket = new Socket( serverHostname, serverPort );
              if ( socket != null )
                {
                  socket.setSoTimeout( 40000 );
                  // Won't compile on SGI... still has java 1.2.2
                  //socket.setKeepAlive(true);
                  bbPrinter.bbNotice( "EventTransporter: Opening object streams on socket\n" );
                  objectsInput = new ObjectInputStream( socket.getInputStream() );
                  objectsOutput = new ObjectOutputStream( socket.getOutputStream() );
                  notConnected = false;
                  bbPrinter.bbNotice( "EventTransporter: Object streams opened successfully\n" );
                }
            }
          catch ( Exception e )
            {
              bbPrinter.bbNotice( "EventTransporter: connection to server " + serverHostname + ":" + serverPort + " failed.\n" );
            }
        }

      tryingToConnect = false;
    }


    /** Set whether or not this transporter is "send only." */
    public void setSendOnly( boolean sendOnly )
    {
      this.sendOnly = sendOnly;
      if ( sendOnly )
        sendEvent( new pdTransportSendOnlyEvent() );
      else
        sendEvent( new pdTransportSendAndReceiveEvent() );
    }


    /** Get whether or not this transporter is "send only." */
    public boolean getSendOnly()
    {
      return sendOnly;
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

      this.getConnection();
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

      // Close the socket.
      try
        {
          socket.close();
          socket = null;
        }
      catch ( Exception e )
        {
          bbPrinter.bbError( "pdEventTransporterTCPIPBridgeImpl: could not stop: " + e.getMessage() + "\n" );
          return false;
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
     * Continuously read events from the socket and send them to the
     * event accepting object.
     */
    public void run()
    {
      pdBasicEvent event = null;

      while ( true )
        {
          event = null;

          try
            {
              event = ( pdBasicEvent ) ( objectsInput.readObject() );
            }
          catch ( java.net.SocketException se )
            {
              // uh oh! lost socket to TCPIPBridge... try to get another one...
              bbPrinter.bbNotice( "EventTransporter: Lost server connection! Trying to reconnect...\n" );
              getConnection();
            }
          catch ( Exception e )
            {
              e.printStackTrace();
            }

          if ( event != null )
            {
              bbPrinter.bbDebug( "Transporter got event: " + event.getEventID() );

              if ( eventAccepter != null )
                eventAccepter.acceptEvent( event );

              if ( isCollectingStats )
                eventStats.countEventReceived( event );
            }

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
      return serverHostname;
    }


    /** Get the IP port this transporter is using. */
    public int getPort()
    {
      return serverPort;
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

      // Don't send unreliably events in a lame effort to keep
      // traffic at a reasonable level for TCP/IP.
      if ( event.getTransportReliably() == false )
        return ;

      // Write out the object to the output stream.
      try
        {
          if ( objectsOutput != null )
            objectsOutput.writeObject( event );
        }
      catch ( java.net.SocketException se )
        {
          // uh oh! lost socket to TCPIPBridge... receiving thread will try to create a new one
          bbPrinter.bbNotice( "EventTransporter: Lost server connection! Trying to reconnect...\n" );
          getConnection();
          sendEvent( event );
          if ( isCollectingStats )
            eventStats.countEventSent( event );
        }
      catch ( Exception e )
        {
          //e.printStackTrace();

        }
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


