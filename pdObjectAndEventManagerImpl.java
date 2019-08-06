package pdObjectSubsystem;

import java.lang.Class;
import java.lang.reflect.Method;
import java.util.*;

import bamboo.bbPrinter;
import bamboo.bbModule;

import pdKernel.*;
import pdObjectSubsystem.events.*;
import pdEventTransportSubsystem.*;
import pdEventTransportSubsystem.events.*;


/**
 * Default implementation of the pdObjectAndEventManager interface.
 *
 * @author BARS Team
 */
public class pdObjectAndEventManagerImpl extends pdEventDispatcherImpl implements pdObjectAndEventManager
  {
    /** ID that should be used for the next registered object. */
    private long nextAvailableObjectID = -1;

    /** A reference to the object repository containing the local object database. */
    private pdObjectRepository objectRepository = null;

    /** Name of the object repository in use */
    private String objectRepositoryName = null;

    /** List of distributed objects we've ever killed */
    Vector killedDistributedObjects = new Vector();

    /** Table of orphan objects and their supposed parents */
    Hashtable orphans = new Hashtable();

    /** 
     * Is this OAEM a "repository holder?" By default, it is, but that status
     * can be revoked at any time. Once revoked it cannot be reinstated.
     */    
    boolean isRepositoryHolder = true;
   

    /**
     * Make a new objectAndEventManager. Use the repository and transporter
     * passed in as parameters.
     */
    public pdObjectAndEventManagerImpl( String name, pdObjectRepository objectRepository, pdChannel channel )
    {
      super( name, channel );

      // Set object repository.
      this.objectRepository = objectRepository;
      this.objectRepositoryName = objectRepository.getName();

      // Determine base for ID assignment
      long positiveRandomNumber = ( new Random() ).nextLong();
      if ( positiveRandomNumber < 0 )
        positiveRandomNumber *= -1;
      nextAvailableObjectID = ( ( positiveRandomNumber % 100000 ) * 100000 ) + 1;

      // Register mandatory event types.
      this.registerEventType( "pdObjectSubsystem.events.pdObjectCreationEvent",
                              "pdObjectSubsystem.events.pdObjectCreationListener",
                              "pdObjectCreated" );
      this.registerEventType( "pdObjectSubsystem.events.pdObjectChangeEvent",
                              "pdObjectSubsystem.events.pdObjectChangeListener",
                              "pdObjectChanged" );
      this.registerEventType( "pdObjectSubsystem.events.pdObjectChangeUnreliableEvent",
                              "pdObjectSubsystem.events.pdObjectChangeListener",
                              "pdObjectChanged" );
      this.registerEventType( "pdObjectSubsystem.events.pdObjectDestructionEvent",
                              "pdObjectSubsystem.events.pdObjectDestructionListener",
                              "pdObjectDestroyed" );
      this.registerEventType( "pdObjectSubsystem.events.pdObjectMessageEvent",
                              "pdObjectSubsystem.events.pdObjectMessageListener",
                              "pdObjectMessaged" );

    }


    /**
     * Make a new non-distributed objectAndEventManager. Use the repository
     * passed in as a parameter.
     */
    public pdObjectAndEventManagerImpl( String name, pdObjectRepository objectRepository )
    {
      this( name, objectRepository, null );
    }


    //////////////////////////////////////////////////////
    //                                                  //
    //  pdObjectAndEventManager method implementations  //
    //                                                  //
    //////////////////////////////////////////////////////

    /**
     * Register an object in the local repository and distribute it
     * if required. Should only be called for LOCALLY-CREATED objects!
     */
    public synchronized void registerObject( pdBaseObject newObject )
    {
      // If the object is dead, then don't assign ID
      if ( newObject.isDead() == true )
        {
          bbPrinter.bbWarn( "pdObjectAndEventManager.registerObject: tried to register dead object ID = "
                            + newObject.getID() + ".\n" );
          return ;
        }

      // If the ID is -1, assign a new one.
      if ( newObject.getID() == -1L )
        {
          //bbPrinter.bbDebug("ObjectAndEventManager.registerObject: Giving new ID to object " + newObject.getName() + ": old = " + newObject.getID() + ", new = " + nextAvailableObjectID + "\n");
          newObject.setID( nextAvailableObjectID );
          nextAvailableObjectID++;
        }

      // If an object with this ID is already registered, don't do anything...
      else if ( objectRepository.findObject( newObject.getID() ) != null )
        {
          bbPrinter.bbWarn( "objectAndEventManager: tried to register object ID = " + newObject.getID() + " but it is already registered.\n" );
          return ;
        }

      bbPrinter.bbDebug( "ObjectAndEventManager.registerObject: Registering object ID = " + newObject.getID() + " name = " + newObject.getName() + " type = " + newObject.getClass().getName() + "\n" );

      if ( objectRepository.registerObject( newObject ) == false )
        {
          bbPrinter.bbError( "ObjectAndEventManager.registerObject: Problem registering object with repository\n");
          bbPrinter.bbError( "ObjectAndEventManager.registerObject: Object ID = " + newObject.getID() + " name = " + newObject.getName() + "\n" );
        }

      // Set the object's reference to the objectAndEventManager
      newObject.setObjectAndEventManager( this );

      // If the object doesn't specify a channel, set it to the
      // active sending channel
      if ( ( newObject.getChannelName() == null ) &&
           ( this.activeSendingChannel != null ) )
        {
          newObject.setChannelName( this.activeSendingChannel.getName() );
        }

      bbPrinter.bbDebug( "ObjectAndEventManager.registerObject: sending creation event for object ID = " + newObject.getID() + "\n" );
      this.acceptEvent( new pdObjectCreationEvent( newObject, false, newObject.getIsDistributed() ) );
    }


    /** Remove an object from the local repository. */
    public synchronized void unregisterObject( pdBaseObject deadObject )
    {
      if ( deadObject == null )
        {
          bbPrinter.bbError( "pdObjectAndEventManagerImpl.unregisterObject: Trying to unregister null object\n" );
          return ;
        }

      // Unregister the object as a listener, if it was one...
      if ( deadObject.getIsAware() )
        this.removeListener( deadObject );

      if ( objectRepository.findObject( deadObject.getID() ) != null )
        {
          bbPrinter.bbDebug( "ObjectAndEventManager.unregisterObject: Unregistering object " + deadObject.getID() + "\n" );
          objectRepository.unregisterObject( deadObject );
        }
    }


    /** 
     * Generate an object ID and reserve it for an object
     * to be registered later (object must have its ID set
     * to the reserved value).
     */      
    public long generateAndReserveID()
    {
      long idToReturn = nextAvailableObjectID;
      nextAvailableObjectID++;
      return idToReturn;
    }
    
 
    /** Find an object in the local repository using its ID. */
    public pdBaseObject findObject( long searchID )
    {
      pdBaseObject findMe = objectRepository.findObject( searchID );

      return findMe;
    }


    /** Find an object in the local repository using its name. */
    public pdBaseObject findObject( String objectName )
    {
      Iterator theObjects = objectRepository.getAllObjects().iterator();

      while ( theObjects.hasNext() )
        {
          Map.Entry nextEntry = ( Map.Entry ) theObjects.next();
          pdBaseObject thisObj = ( pdBaseObject ) ( nextEntry.getValue() );
          if ( ( thisObj.getName() != null ) && ( thisObj.getName().equals( objectName ) ) )
            return thisObj;
        }

      return null;
    }


    /** Return a set of all objects. */
    public Set getAllObjects()
    {
      return objectRepository.getAllObjects();
    }
    

    /** 
     * We found an orphan; tell the oaem so that it can reunite
     * the objects when the parent is found.
     */                                                         
    public void addOrphan( long childID, long parentID )
    {
      if ( orphans.contains( new Long( childID ) ) )
        {
          long oldParentID = ( ( Long )( orphans.get( new Long( childID ) ) ) ).longValue();
          bbPrinter.bbDebug( "objectAndEventManager: already have orphan " +
                             childID + " with parent " + oldParentID + " (new parent = " +
                             parentID + ")... ignoring this request\n" );
        }
      else
        {
          bbPrinter.bbDebug( "objectAndEventManager: adding orphan " + childID + 
                             " with deadbeat parent " + parentID + "\n" );
          orphans.put( new Long( childID ), new Long( parentID ) );
        }
    }
    

    /** Check if this object is a deadbeat parent and if so, look for its child to reunite them. */
    protected void checkAndReuniteParent( pdBaseObject parent )
    {
      if ( orphans.containsValue ( new Long( parent.getID() ) ) )
        {
          // Walk through all children and see how many might need this parent.
          Enumeration e = orphans.keys();
          while ( e.hasMoreElements() )
            {            
              Long currChildID = ( Long )( e.nextElement() );
              Long currParentID = ( Long )( orphans.get( currChildID ) );
              
              // If this child needs this parent, reunite them and remove this orphans table entry.
              if ( currParentID.longValue() == parent.getID() )
                {
                  bbPrinter.bbNotice( "objectAndEventManager: reuniting child " + currChildID + 
                                      " with parent " + currParentID + "\n" );
                                      
                  pdBaseObject child = findObject( currChildID.longValue() );
                  if ( child instanceof pdCoordinateTreeObject )
                    {
                      ( ( pdCoordinateTreeObject ) child ).setParentLocal( parent.getID() );
                    }
                  
                  orphans.remove( currChildID );
                }
            }
        }
      else
        {
          return;
        }
      
    }


    /** Is this OAEM a "repository holder?" */
    public boolean getIsRepositoryHolder()
    {
      return isRepositoryHolder;
    }
    
    
    /** Revoke this OAEM's "repository holder" privileges. */
    public void revokeRepositoryHolderStatus()
    {
      this.isRepositoryHolder = false;
      
      bbPrinter.bbDebug( "ObjectAndEventManager: I AM NO LONGER A REPOSITORY HOLDER! \n" );
    }
        
    
    
    ////////////////////////////////////////////////////////////
    //                                                        //
    //  pdEventDispatcherImpl abstract method implementation  //
    //                                                        //
    ////////////////////////////////////////////////////////////

    /** Accept an event to be dispatched */
    public synchronized void acceptEvent( pdBasicEvent event )
    {
      // Don't dispatch null events
      if ( event == null )
        return ;
   
      // Handle transport system events...
      
      // If the event says to revoke "repository holder" status, then do it, maybe.
      if ( event instanceof pdObjectAndEventManagerRevokeRepositoryHolderStatusEvent )
        {
          // Act on this event if it's to this application or to all objects (ignore if it
          // is specifically to some other application).
          if ( ( ( objectRepository.getSelfUserObject() != null ) && ( event.getTargetID() == objectRepository.getSelfUserObject().getID() ) ) ||
               ( event.getTargetID() == pdObjectEvent.ALLOBJECTS ) )
            {            
              revokeRepositoryHolderStatus();
            }
            
          return;
        }
          
      
      // Send creation events for all existing distributed objects for the
      // benefit of the new network member.
      // Make sure this object is the target.
      if ( event instanceof pdObjectAndEventManagerSendAllObjectsEvent )
        {
          // Respond to this event IF the event is specifically to this application
          // OR it's to a "repository holder" and this app is designated as a "repository holder."
          if ( ( ( objectRepository.getSelfUserObject() != null ) && ( event.getTargetID() == objectRepository.getSelfUserObject().getID() ) ) ||
               ( ( event.getTargetID() == pdObjectAndEventManagerEvent.REPOSITORY_HOLDER ) && getIsRepositoryHolder() ) )
            {
              bbPrinter.bbDebug( "ObjectAndEventManager: SENDING ALL OBJECTS AND DEATH NOTICES!!!!!\n" );
              
              // Get the channel for this event
              String channelName = event.getChannelName();

              // Keep track of which objects we've sent
              Vector sentList = new Vector();

              // Send the objects
              Iterator allObjects = ( objectRepository.getAllObjects() ).iterator();
              while ( allObjects.hasNext() )
                {
                  pdBaseObject currObject = ( pdBaseObject ) ( ( ( Map.Entry
                                                                 ) allObjects.next() ).getValue() );
                  sendObjectReplica( currObject, channelName, sentList );
                }
              
              // This condition occurs usually when another application has joined the
              // network OR has re-joined the network after leaving it. In the latter case,
              // that other application may have also missed object kill events (in addition
              // to the object create events we just re-sent). So, send destruction events 
              // (probably redundant) for all destroyed objects.
              Iterator koit = killedDistributedObjects.iterator();
              while ( koit.hasNext() )
                {
                  long killMe = ( ( Long ) koit.next() ).longValue();
                  acceptEvent( new pdObjectDestructionEvent( killMe, false, true ) );
                }
              
            }
          return ;
        }


      // If we get an "identify object" event, send out a copy of that object,
      // if we have the original.
      if ( event instanceof pdObjectAndEventManagerIdentifyObjectEvent )
        {
          bbPrinter.bbDebug("ObjectAndEventManager: got identify message for object " + event.getTargetID() + "\n");
          pdBaseObject wantedObject = findObject( event.getTargetID() );
          
          if ( wantedObject == null )
          {
            bbPrinter.bbDebug("ObjectAndEventManager.acceptEvent: I do not have object " + event.getTargetID() + "\n");
            return;
          }
            
          // If the object isn't original to this application, don't send it.
          if ( wantedObject.getIsRemoteCopy() ) 
          {
            bbPrinter.bbDebug("ObjectAndEventManager.acceptEvent: NOT sending creation message for " + event.getTargetID() + " because it's not original\n");
            return;
          }
          
          // Otherwise, send it back.
          if ( wantedObject != null )
            {
              bbPrinter.bbNotice("ObjectAndEventManager.acceptEvent: sending creation message for " + event.getTargetID() + "\n");
              super.acceptEvent( new pdObjectCreationEvent( wantedObject, false, wantedObject.getIsDistributed() ) );
              
              // Also send its children if they're distributed.
              if ( wantedObject instanceof pdCoordinateTreeObject ) 
                {
                  sendDistributedChildren( ( ( pdCoordinateTreeObject ) wantedObject ).getAllChildIDs() );
                }
                
              // This condition is reached mainly when the computer holding
              // this active object has been disconnected from the network. 
              // During this disconnection, some object updates might have
              // been missed. So, also send request for ALL OBJECTS since we 
              // might have missed some during our absence from the network.
              if ( defaultChannel != null )
                defaultChannel.sendEvent( new pdObjectAndEventManagerSendAllObjectsEvent() );
            }
          return ;
        }


      // If we get an "are you alive" event, respond, if the target is the
      // user object of this application.
      if ( event instanceof pdObjectAndEventManagerAreYouAliveEvent )
        {
          bbPrinter.bbDebug("ObjectAndEventManager: got 'are you alive' message for object " + event.getTargetID() + "\n");
          
          if ( ( objectRepository.getSelfUserObject() != null ) &&
               ( event.getTargetID() == objectRepository.getSelfUserObject().getID() ) )
            {
              //bbPrinter.bbDebug("ObjectAndEventManager: replying to 'are you alive' message for " + event.getTargetID() + "\n");
              super.acceptEvent( new pdObjectCreationEvent( objectRepository.getSelfUserObject(), false, objectRepository.getSelfUserObject().getIsDistributed() ) );
            }
          return ;
        }

      // If this is not an object event and we haven't
      // dealt with it yet, just dispatch it and return.
      if ( !( event instanceof pdObjectEvent ) )
        {
          super.acceptEvent( event );
          return ;
        }

      // If this is an object event, deal with it, otherwise we're done.
      pdObjectEvent objEvent = null;
      if ( event instanceof pdObjectEvent )
        objEvent = ( pdObjectEvent ) event;
      else
        return ;
      
      // Handle object creation
      // SJJ: 26/06/01
      // Unfortunately, it appears that creation events always have to be
      // dispached locally, whether or not the object is already
      // registered in the repository....
      if ( objEvent instanceof pdObjectCreationEvent )
        {
          handleCreationEvent( ( pdObjectCreationEvent ) objEvent );
        }

      // Fill in the source and target objects into the transient fields
      // of the event.
      objEvent.setSourceObject( findObject( objEvent.getSourceID() ) );
      objEvent.setTargetObject( findObject( objEvent.getTargetID() ) );

      // Handle object destruction
      if ( objEvent instanceof pdObjectDestructionEvent ) 
        {
          // Ignore outside requests to kill user object or its descendants!
          if ( ( ! objEvent.getIsSourceLocal() ) && 
               ( objectRepository.getSelfUserObject() != null ) &&
               ( ( event.getTargetID() == objectRepository.getSelfUserObject().getID() ) ||
               ( isAncestorOf( objectRepository.getSelfUserObject(), objEvent.getTargetObject() ) ) ) )
            {
              bbPrinter.bbNotice( "objectAndEventManager: Ignoring request to kill user object or a descendant: " 
                                  + event.getTargetID() + "\n" );
              return ;
            }
          else
            {
              // When we get a destruction event, put the destroyed object's ID
              // into the list of killed objects IF it's a distributed NON-active object.
              // We don't care about non-distributed objects (we killed it locally, no one
              // else will bring it back to life). We exclude user objects because they
              // CAN bring themselves back to life and we don't want bouncing "alive-dead-alive-dead"
              // messags clogging up the system. 
              
              // If ID is not already in the list, add it to the list.
              if ( ! killedDistributedObjects.contains( new Long( objEvent.getTargetID() ) ) ) 
                {
                  // Check if object is distributed.
                  if ( ( objEvent.getTargetObject() != null ) && 
                       ( objEvent.getTargetObject().getIsDistributed() ) )
                    {
                      killedDistributedObjects.add( new Long( objEvent.getTargetID() ) );                  
                    }
                }
            }
        }

      // Once again, folks, it's STUPID HACK TIME!!!! Yay!!!!
      // If this is a change event from an object to itself, dispatch
      // it immediately! Don't let it linger on the evil event queue!
      // Later dispatching code has a symmetric hack to not dispatch
      // this event to the same target again.
      if ( event instanceof pdObjectChangeEvent )
        {
          pdObjectChangeEvent changeEvent = ( pdObjectChangeEvent )event;
          if ( changeEvent.getSourceID() == changeEvent.getTargetID() )
            {
              pdBaseObject target = objEvent.getTargetObject();
              if ( target != null )
                {
                  target.pdObjectChanged( changeEvent );
                }
            }
        }


      // Now accept the event
      super.acceptEvent( event );
    }

     
    /**
     * Is child an ancestor of the object?
     */
    private boolean isAncestorOf( pdBaseObject ancestor, pdBaseObject child )
    {
      // If either one is null, return false.
      if ( ( ancestor == null) || ( child == null ) ) return false;

      // If either one isn't a coord tree ancestor, they are not in parent/child 
      // relationship.
      if ( ( ! ( ancestor instanceof pdCoordinateTreeObject ) ) ||
           ( ! ( child instanceof pdCoordinateTreeObject ) ) )
        return false;
      pdCoordinateTreeObject cAncestor = ( pdCoordinateTreeObject )ancestor;
      pdCoordinateTreeObject cChild = ( pdCoordinateTreeObject )child;

      // Walk through the child's ancestors and see if we hit the ancestor.     
      pdCoordinateTreeObject currAncestor = ( pdCoordinateTreeObject )( cChild.getParent() );
      while ( currAncestor != null ) 
        {
          // Did we find the test ancestor? If so, jackpot!
          if ( currAncestor.getID() == cAncestor.getID() ) return true;
          
          // Otherwise move on the to the next one.
          else currAncestor = ( pdCoordinateTreeObject )( currAncestor.getParent() );
        }
   
      // If we got this far, we went through all of the child's ancestors
      // without finding the ancestor, so we can conclude there is
      // no ancestor relationship.
      return false;      
    }


    /**
     * Send creation events for distributed children of an object.
     * Recursive; terminates on empty iterator.
     */
    private void sendDistributedChildren( Iterator children )
    {
      while ( children.hasNext() )
        {
          long childID = ( ( Long ) children.next() ).longValue();
          pdBaseObject child = findObject( childID );
          if ( child == null )
            {
              bbPrinter.bbError( "ObjectAndEventManager.sendDistributedChildren: child " + childID + " cannot be found!\n");
              continue;
            }
          else
            {
              if ( child.getIsDistributed() )
                {
                  bbPrinter.bbNotice("ObjectAndEventManager.sendDistributedChildren: sending creation message for " + childID + "\n");
                  super.acceptEvent( new pdObjectCreationEvent( child, false, true ) );
                  if ( child instanceof pdCoordinateTreeObject )
                    {
                      sendDistributedChildren( ( ( pdCoordinateTreeObject ) child ).getAllChildIDs() );
                    }
                }
            }
        }
    }


    /**
     * Send an object replica over the given channel if the object is
     * distributed and belongs to the channel and it hasn't been sent yet
     * (i.e. isn't in sentList).
     */
    private void sendObjectReplica( pdBaseObject currObject, String channelName, Vector sentList )
    {
      // Check if object is in sent list; if so, skip it.
      if ( sentList.contains( currObject ) )
        return ;

      // If this object has a parent, make sure to send it first.
      // HACK: current code can send an object many, many times since
      // we don't keep track of objects we've sent. Worst case:
      // pass a leaf into this method...
      if ( currObject instanceof pdCoordinateTreeObject )
        {
          pdCoordinateTreeObject treeObject = ( pdCoordinateTreeObject ) currObject;
          if ( treeObject.getParentID() != pdCoordinateTreeObject.NO_PARENT )
            {
              sendObjectReplica( findObject( treeObject.getParentID() ), channelName, sentList );
            }
        }

      // Can send it if it's distributed and the channel name matches
      // the event channel name.
      if ( currObject.getIsDistributed() &&
           ( ( channelName != null ) &&
             channelName.equals( currObject.getChannelName() ) ) )
        {
          pdObjectCreationEvent creationEvent = new pdObjectCreationEvent( currObject, false, true );
          super.acceptEvent( creationEvent );
          sentList.add( currObject );
        }
    }


    /** Set the default channel */
    public void setDefaultChannel( pdChannel newDefaultChannel )
    {
      super.setDefaultChannel( newDefaultChannel );

      // Ask for existing objects on this channel.
      if ( this.defaultChannel != null )
        {
          defaultChannel.sendEvent( new pdObjectAndEventManagerSendAllObjectsEvent() );
        }
    }


    /** Join a channel */
    public void joinChannel( pdChannel channel )
    {
      // Really join the channel
      super.joinChannel( channel );

      // Ask for existing objects on channel
      channel.sendEvent( new pdObjectAndEventManagerSendAllObjectsEvent() );
    }


    ////////////////////////////////////////////////////////////
    //                                                        //
    //  pdActiveSystemObject abstract method implementations  //
    //                                                        //
    ////////////////////////////////////////////////////////////

    /** Start the accessory threads. */
    protected boolean _start()
    {
      boolean result = super._start();
      if ( result == true )
        {
          bbPrinter.bbDebug( "ObjectAndEventManager: ASKING FOR ALL OBJECTS NOW\n" );
          if ( defaultChannel != null )
            defaultChannel.sendEvent( new pdObjectAndEventManagerSendAllObjectsEvent() );
        }

      return result;
    }


    ///////////////////////////////////////////
    //                                       //
    //  pdObjectAndEventManagerImpl methods  //
    //                                       //
    ///////////////////////////////////////////

    /**
     * When we get a creation event, we must set some fields of the new object
     * so that it fits in locally as well as register it in the local repository.
     */
    private void handleCreationEvent( pdObjectCreationEvent event )
    {
      // Creation events are distributed locally if and only if the object
      // has not already been registered with the objectRepository.
      pdBaseObject newObject = event.getNewObject();

      // If object is null, stop...
      if ( newObject == null )
        return ;

      // If the newly created object is on our killed list (if it were
      // resurrected) take it off the killed list.
      if ( killedDistributedObjects.contains( new Long( newObject.getID() ) ) )
        {
          killedDistributedObjects.remove( new Long( newObject.getID() ) );
        }

      // Check is this is a deadbeat parent and reunite it with its orphan children.
      checkAndReuniteParent( newObject );

      // If we already have an object with this ID, stop...
      if ( objectRepository.findObject( newObject.getID() ) != null )
        { 
          bbPrinter.bbDebug( "ObjectAndEventManager: Got creation event for object I already have: " + newObject.getID() + "\n" );
          return ;
        }

      bbPrinter.bbNotice( "ObjectAndEventManager.handleCreationEvent: Registering new foreign object ID = " + newObject.getID() + "\n" );

      // Set some local values on the object...
      newObject.setIsRemoteCopy( true );
      newObject.setIsDistributedLocal( true );
  
      // Set the object's objectAndEventManager object and register it in the repository.
      objectRepository.registerObject( newObject );
      newObject.setObjectAndEventManager( this );
    }

    
    /** debug printing stuff */
    private static int debugLevel = bbPrinter.NO_LEVEL;
    static
      {
        debugLevel = bbPrinter.getLevel( bbModule.getCurrentModule().getName() );
      }
  }



