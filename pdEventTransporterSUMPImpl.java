package pdEventTransportSubsystem;

import java.io.*;
import java.net.*;
import java.util.*;

import bamboo.bbPrinter;

import pdEventTransportSubsystem.events.*;
import pdKernel.*;


/**
 * SUMP: Selectively Unreliable Multicast Protocol. This "protocol"
 * consists of regular IP multicast and LRMP. Since LRMP is slow,
 * we sometimes want the speed of IP multicast. So we send important
 * ("reliable") events using LRMP and replaceable ("unreliable") events 
 * using IP multicast.
 *
 * @author BARS Team
 */
public class pdEventTransporterSUMPImpl extends pdActiveSystemObject
      implements pdEventTransporter, pdEventAccepter
  {
    /** The event-accepting object. */
    transient protected pdEventAccepter eventAccepter = null;

    /** The IP Multicast transporter. */
    transient protected pdEventTransporterIPMulticastImpl IPMulticastTransporter = null;

    /** The LRMP transporter. */
    transient protected pdEventTransporterLRMPImpl LRMPTransporter = null;

    /** Multicast address to use */
    protected String multicastAddress = null;

    /** Multicast port to use */
    protected int port = -1;

    /** currently collecting statistics? */
    transient protected boolean isCollectingStats = false;

    /** statistics we might be collecting */
    transient pdEventTransporterStatistics eventStats = 
      new pdEventTransporterStatistics( "SUMP" );

    /** 
     * Number of IPMulticast (unreliable) events we've seen since the last LRMP  
     * (reliable) event. If this number exceeds some threshhold, we assume the
     * LRMP transporter is locked up so we kill it and make a new one. 
     */
    transient protected long LRMPWatchdogCounter = 0;

    /** Threshhold described in previous comment for LRMPWatchdogCounter. */                                                  
    protected long LRMPWatchdogThreshhold = 30;
    
        
    /** Create a new SUMP transporter using this multicast address and port. */
    public pdEventTransporterSUMPImpl( String multicastAddress, int port )
    throws Exception
      {
        this.multicastAddress = multicastAddress;
        this.port = port;
        IPMulticastTransporter = new pdEventTransporterIPMulticastImpl( multicastAddress, port );
        LRMPTransporter = new pdEventTransporterLRMPImpl( multicastAddress, port + 1 );
        
        // Check command line args
        String LRMPWatchdogThreshholdString = CommandLine.get( "LRMPWatchdogThreshhold", "30" );
        try
          {
            LRMPWatchdogThreshhold = Long.parseLong( LRMPWatchdogThreshholdString );
          }
        catch ( NumberFormatException e )
          {
            bbPrinter.bbError( "LRMPWatchdogThreshhold not a number! Using 30...\n" );
            LRMPWatchdogThreshhold = 30;
          }
      }


    /**
     * Initialize a remote copy of this transporter.
     */
    public void initializeRemoteCopy() throws Exception
      {
        IPMulticastTransporter = new pdEventTransporterIPMulticastImpl( multicastAddress, port );
        LRMPTransporter = new pdEventTransporterLRMPImpl( multicastAddress, port + 1 );
        this.status = pdActiveObject.STOPPED;
      }


    ////////////////////////////////////////////////////////////
    //                                                        //
    //  pdActiveSystemObject abstract method implementations  //
    //                                                        //
    ////////////////////////////////////////////////////////////

    /** Start the threads of each transporter. */
    protected boolean _start()
    {
      if ( eventAccepter == null )
        {
          bbPrinter.bbError( "Cannot start transporter: event accepter is null.\n" );
          return false;
        }

      return ( IPMulticastTransporter._start() && LRMPTransporter.start() );
    }


    /** Stop the threads of each transporter. */
    protected boolean _stop()
    {
      IPMulticastTransporter.stop();
      LRMPTransporter.stop();
      return true;
    }


    /** Suspend the threads of each transporter. */
    protected boolean _suspend()
    {
      IPMulticastTransporter.suspend();
      LRMPTransporter.suspend();
      return false;
    }


    /** Resume the threads of each transporter. */
    protected boolean _resume()
    {
      IPMulticastTransporter.resume();
      LRMPTransporter.resume();
      return false;
    }


    //////////////////////////////////////////////
    //                                          //
    //  pdEventAccepter method implementations  //
    //                                          //
    //////////////////////////////////////////////

    /** Accept an event to be dispatched */
    public void acceptEvent( pdBasicEvent event )
    {
      //System.out.print( "SUMP transporter got event, passing it on... " );
      if ( event.getTransportReliably() )
        {
          //System.out.println(" probably from LRMP");
          LRMPWatchdogCounter = 0;
        }
      else
        {
          //System.out.println(" probably from IPMulticast");        
          LRMPWatchdogCounter++;
        }
      
      if ( LRMPWatchdogCounter >= LRMPWatchdogThreshhold )
      {
        bbPrinter.bbWarn( "LRMP transporter seems locked up--resetting...\n" );
        LRMPTransporter.stop();
        try 
          {
            LRMPTransporter = new pdEventTransporterLRMPImpl( multicastAddress, port + 1 );
            LRMPTransporter.setEventAccepter( this );
            LRMPTransporter.start();
          }
        catch ( Exception e )
          {
            bbPrinter.bbError( "LRMP transporter could not be created--you should restart this application.\n" );
            e.printStackTrace();
          }
        LRMPWatchdogCounter = 0;
      }
                                                           
      this.eventAccepter.acceptEvent( event );    
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
      IPMulticastTransporter.setEventAccepter( this );
      LRMPTransporter.setEventAccepter( this );
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
      bbPrinter.bbDebug( "SUMP Transporter sending " + event.getClass().getName() + ": " + event.toString() + "\n" );

      if ( event.getTransportReliably() )
        {
          LRMPTransporter.sendEvent( event );
        }
      else
        {
          IPMulticastTransporter.sendEvent( event );
        }
    }


    /** Turn on statistics-collecting. */
    public void startCollectingStatistics()
    {
      LRMPTransporter.startCollectingStatistics();
      IPMulticastTransporter.startCollectingStatistics();
    }


    /** Turn off statistics-collecting. */
    public void stopCollectingStatistics()
    {
      LRMPTransporter.stopCollectingStatistics();
      IPMulticastTransporter.stopCollectingStatistics();
    }

    
    /** Return statistics for this transporter. */
    public pdEventTransporterStatistics getStatistics()
    {
      pdEventTransporterStatistics LRMPStats = LRMPTransporter.getStatistics();
      pdEventTransporterStatistics IPMulticastStats = IPMulticastTransporter.getStatistics();
      LRMPStats.peerStats = IPMulticastStats;
      return LRMPStats;
    }

 
    //////////////////////////////////////////
    //                                      //
    //  pdEventTransporterSUMPImpl methods  //
    //                                      //
    //////////////////////////////////////////
    
    /** Get the IPMulticast transporter */
    public pdEventTransporterIPMulticastImpl getIPMulticastTransporter()
    {
      return IPMulticastTransporter;
    }


    /** Get the LRMP transporter */
    public pdEventTransporterLRMPImpl getLRMPTransporter()
    {
      return LRMPTransporter;
    }
  }


