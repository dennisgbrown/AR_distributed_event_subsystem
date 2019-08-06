package pdEventTransportSubsystem;

import java.io.Serializable;
import java.lang.Exception;

import pdEventTransportSubsystem.events.*;
import pdKernel.*;


/**
 * EventTransporter interface
 *
 * @author BARS Team
 */
public interface pdEventTransporter extends pdSystemObject, pdActiveObject, Serializable
  {
    /**
     * Initialize a remote copy of this transporter.
     */
    public void initializeRemoteCopy() throws Exception;


    /** Set the object that will accept events received. */
    public void setEventAccepter( pdEventAccepter eventAccepter );


    /** Get the object that will accept events received. */
    public pdEventAccepter getEventAccepter();


    /** Get the IP address this transporter is using. */
    public String getAddress();


    /** Get the IP port this transporter is using. */
    public int getPort();


    /** Send an event to the network. */
    public void sendEvent( pdBasicEvent event );


    /** Turn on statistics-collecting for this transporter. */
    public void startCollectingStatistics();


    /** Turn off statistics-collecting for this transporter. */
    public void stopCollectingStatistics();
    

    /** Return statistics for this transporter. */
    public pdEventTransporterStatistics getStatistics();
  }


