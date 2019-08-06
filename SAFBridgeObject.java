package safBridge;

import java.util.*;
import javax.media.j3d.*;
import javax.vecmath.*;

import geotransform.coords.*;
import geotransform.transforms.*;
import geotransform.ellipsoids.*;

import bamboo.*;

import pdKernel.*;
import pdObjectSubsystem.events.*;
import pdObjectSubsystem.*;
import pdEventTransportSubsystem.*;
import pdRendererSubsystem.graphics.*;

import barsApp.*;
import barsObjects.*;
import barsObjects.users.*;
import barsObjects.linesegments.*;
import barsObjects.forces.*;
import barsObjects.infrastructure.*;


/**
 * The SAF Bridge Object
 *
 * @author BARS Team
 */
public class SAFBridgeObject extends pdActiveSystemObject
implements pdObjectCreationListener, pdObjectChangeListener,
pdObjectDestructionListener
{
  /** The ever-present object and event mangler! */
  static pdObjectAndEventManager objectAndEventManager = null;

  /** List of IDs of BARS Objects created by the SAF */
  static Vector BARSObjectIDsCreatedBySAF = new Vector();

  /** What protocol are we using to talk to SAF? */
  private String protocol = null;


  /** Lat-lon point that matches BARS origin */
  private static Gdc_Coord_3d latlonMapCenter = null;
 
  /** UTM point that matches BARS origin */
  private static Utm_Coord_3d utmMapCenter = null;


  /** Initialize the application. */
  public static boolean initPlugin()
  {
    try
    {
      bbPrinter.bbNotice( "Trying to load native library safBridgeModule...\n" );
      System.loadLibrary( "safBridgeModule" );
      bbPrinter.bbNotice( "...successful\n" );
    }
    catch ( Exception e )
    {
      e.printStackTrace();
    }

    return true;
  }


  /** Exit the application. */
  public static boolean exitPlugin()
  {
    return true;
  }


  /**
   * Create new SAF interface.
   */
  public SAFBridgeObject( pdObjectAndEventManager objectAndEventManager )
  {
    this( null, objectAndEventManager );
  }


  /**
   * Create new SAF interface with a name.
   */
  public SAFBridgeObject( String name, pdObjectAndEventManager objectAndEventManager )
  {
    super( name );

    bbPrinter.bbDebug( "safBridge: Created.\n" );

    // Initialize coordinate conversion stuff
    Gdc_To_Utm_Converter.Init( new WE_Ellipsoid() );
    Utm_To_Gdc_Converter.Init( new WE_Ellipsoid() );
    latlonMapCenter = getLatlonMapCenter();
    utmMapCenter = new Utm_Coord_3d();
    Gdc_To_Utm_Converter.Convert( latlonMapCenter, utmMapCenter );
    //System.out.print( "  Latlon map center: " + latlonMapCenter.latitude + "," + latlonMapCenter.longitude + "," + latlonMapCenter.elevation + "\n" );
    //System.out.print( "  UTM map center: " + utmMapCenter.x + "," + utmMapCenter.y + "," + utmMapCenter.z + "\n" );

    // Set "main" objectAndEventManager
    this.objectAndEventManager = objectAndEventManager;

    // Start interface as specified on command line
    protocol = CommandLine.get( "protocol", "DIS" );

    // *** SPECIAL CASE ALERT!!!! ***
    // If DIS, create a new DIS Interface which is, itself, an
    // activeSystemObject with its own event listeners.
    if ( protocol.equalsIgnoreCase( "DIS" ) )
    {
      DISInterface di = new DISInterface( objectAndEventManager );
      di.start();
    }

    // If HLA, start the native side and handle events in this object.
    else if ( protocol.equalsIgnoreCase( "HLA" ) )
    {
      String hlaType = CommandLine.get( "hlaType", "TEST" );

      // Start native side
      nativeStart( hlaType );

      // Send existing objects to SAF side
      sendExistingBARSObjects();

      // Register as event listener
      objectAndEventManager.registerListener( this );
    }
  }


  /** Read latlon map center from command line */
  private Gdc_Coord_3d getLatlonMapCenter()
  {
    // Default location is BARS origin.
    double latCenter = 38.82053089;
    double lonCenter = -77.02538335;
    double altCenter = -11.8686;
    String latlonMapCenterString = CommandLine.get( "latlonMapCenter" );
    if ( latlonMapCenterString != null )
    {
      try
      {
        String latString = null;
        String lonString = null;
        String altString = null;
        StringTokenizer strtok = new StringTokenizer( latlonMapCenterString, "," );
        if ( strtok.hasMoreTokens() ) latString = strtok.nextToken();
        if ( strtok.hasMoreTokens() ) lonString = strtok.nextToken();
        if ( strtok.hasMoreTokens() ) altString = strtok.nextToken();
        if ( latString != null ) latCenter = Double.parseDouble( latString );
        if ( lonString != null ) lonCenter = Double.parseDouble( lonString );
        if ( altString != null ) altCenter = Double.parseDouble( altString );
      }
      catch ( Exception e )
      {
        e.printStackTrace();
      }
    }

    //System.out.println("latlonMapCenter is " + latCenter + "," + lonCenter + "," + altCenter );
    Gdc_Coord_3d latlon = new Gdc_Coord_3d( latCenter, lonCenter, altCenter );

    return latlon;  
  }


  /** Given a latlon, return a BARS position */  
  public static Vector3d getBARSPositionFromLatlon( Gdc_Coord_3d latlon )
  {
    if ( utmMapCenter == null ) 
    {
      bbPrinter.bbError( "Tried to use getBARSPositionFromLatlon before initialization.\n" );
      return null;
    }
    
    Utm_Coord_3d utm = new Utm_Coord_3d();
    Gdc_To_Utm_Converter.Convert( latlon, utm );
    Vector3d barsPosition = new Vector3d( ( utm.x - utmMapCenter.x ), ( utm.y - utmMapCenter.y ), ( utm.z - utmMapCenter.z ) );

    //System.out.println("MAKE BARS POSITION");
    //System.out.println("latlon loc is " + latlon.latitude + "," + latlon.longitude + "," + latlon.elevation );
    //System.out.println("UTM loc is " + utm.x + "," + utm.y + "," + utm.z );
    //System.out.println("BARS loc is " + barsPosition );
                       
    return barsPosition;
  }


  /** Given a BARS position, return a latlon */  
  public static Gdc_Coord_3d getLatlonFromBARSPosition( Vector3d position )
  {
    if ( utmMapCenter == null ) 
    {
      bbPrinter.bbError( "Tried to use getLatlonFromBARSPosition before initialization.\n" );
      return null;
    }
    
    Utm_Coord_3d utm = new Utm_Coord_3d();
    utm.x = position.x + utmMapCenter.x;
    utm.y = position.y + utmMapCenter.y;
    utm.z = position.z + utmMapCenter.z;
    utm.hemisphere_north = utmMapCenter.hemisphere_north; 
    utm.zone = utmMapCenter.zone; 
    Gdc_Coord_3d latlon = new Gdc_Coord_3d();
    Utm_To_Gdc_Converter.Convert( utm, latlon );

    //System.out.println("MAKE LATLON");
    //System.out.println("BARS loc is " + position );
    //System.out.println("UTM loc is " + utm.x + "," + utm.y + "," + utm.z );
    //System.out.println("latlon loc is " + latlon.latitude + "," + latlon.longitude + "," + latlon.elevation + "\n" );
                       
    return latlon;
  }


  /**
   * Find out what BARS objects were created before we started
   * listening for them, and send them to SAF.
   */
  protected void sendExistingBARSObjects()
  {
    Iterator theObjects = objectAndEventManager.getAllObjects().iterator();

    while ( theObjects.hasNext() )
    {
      Map.Entry nextEntry = ( Map.Entry ) theObjects.next();
      BARSObject thisObj = ( BARSObject ) ( nextEntry.getValue() );
      nativeBARSObjectCreated( thisObj.getClass().getName(),
                               "" + thisObj.getID(),
                               thisObj.getPosition().x,
                               thisObj.getPosition().y,
                               thisObj.getPosition().z,
                               thisObj.getOrientation().x,
                               thisObj.getOrientation().y,
                               thisObj.getOrientation().z );
    }
  }


  /** Respond to object creation. */
  public void pdObjectCreated( pdObjectCreationEvent creationEvent )
  {
    // If this is just a local echo, ignore it...
    if ( creationEvent.getIsSourceLocal() == true )
      return;

    BARSObject newObject = ( BARSObject ) ( creationEvent.getNewObject() );

    // Ignore non-distributed objects (like world, route parts, etc)
    if ( !newObject.getIsDistributed() )
      return ;

    if ( BARSObjectIDsCreatedBySAF.contains( new Long( newObject.getID() ) ) )
    {
      bbPrinter.bbNotice( "JAVA SIDE: Blocked sending of object " + newObject.getID() + "\n" );
      return ;
    }

    Gdc_Coord_3d latlon = getLatlonFromBARSPosition( newObject.getPosition() );

    nativeBARSObjectCreated( newObject.getClass().getName(),
                             "" + newObject.getID(),
                             latlon.latitude,
                             latlon.longitude,
                             latlon.elevation,
                             newObject.getOrientation().x,
                             newObject.getOrientation().y,
                             newObject.getOrientation().z );
  }


  /** Respond to object change. */
  public void pdObjectChanged( pdObjectChangeEvent changeEvent )
  {
    // If this is just a local echo, ignore it...
    if ( changeEvent.getIsSourceLocal() == true )
      return;

    BARSObject changedObject = ( BARSObject ) ( changeEvent.getTargetObject() );

    if ( changedObject == null )
      {
        bbPrinter.bbWarn( "SAFBridgeObject: got change for null object " + changeEvent.getTargetID() + "\n" );
        return;
      }

    // Get latlon to put in change method call
    Gdc_Coord_3d latlon = getLatlonFromBARSPosition( changedObject.getPosition() );

    // If this is a property change event, pass along the property name and value
    String propertyName = "null";
    String propertyValue = "null";
    if ( changeEvent.getMethodName().equals( BARSObject.SET_PROPERTY ) )
      {
        propertyName = ( String)( ( changeEvent.getParameters() )[0] );
        propertyValue = ( String)( ( changeEvent.getParameters() )[1] );
      }

    nativeBARSObjectChanged( "" + changedObject.getID(),
                             latlon.latitude,
                             latlon.longitude,
                             latlon.elevation,
                             changedObject.getOrientation().x,
                             changedObject.getOrientation().y,
                             changedObject.getOrientation().z,
                             propertyName,
                             propertyValue );
  }


  /** Respond to object destruction. */
  public void pdObjectDestroyed( pdObjectDestructionEvent destructionEvent )
  {
    // If this is just a local echo, ignore it...
    if ( destructionEvent.getIsSourceLocal() == true )
      return;

    nativeBARSObjectDestroyed( "" + destructionEvent.getTargetID() );
  }


  /** Create a new BARS object -- invoked from native side */
  public synchronized static String createBARSObject( String type,
                                                      double posX, double posY, double posZ,
                                                      double oriX, double oriY, double oriZ )
  {
    bbPrinter.bbNotice( "JAVA SIDE: Request to create a BARS object from SAF side: " + type + ": " +
                        "( " + posX + ", " + posY + ", " + posZ + " ) " +
                        "( " + oriX + ", " + oriY + ", " + oriZ + " )\n " );

    long newID = -1;

    Vector3d position = getBARSPositionFromLatlon( new Gdc_Coord_3d( posX, posY, posZ ) );

    // Try to instantiate the class named by the type
    Class aClass = null;
    try
      {
        aClass = Class.forName( type );
      }
    catch ( Exception e )
      {
        bbPrinter.bbError( "JAVA SIDE: Can't find class named " + type + "\n" );
        e.printStackTrace();
        return null;
      }
    Object anObject = null;
    try
      {
        anObject = aClass.newInstance();
      }
    catch ( Exception e )
      {
        bbPrinter.bbError( "JAVA SIDE: Can't instantiate class named " + type + "\n" );
        e.printStackTrace();
        return null;
      }
    if ( ! ( anObject instanceof BARSObject ) )
      {
        bbPrinter.bbError( "JAVA SIDE: Class named " + type + " isn't a BARS object!\n" );
        return null;
      }
    BARSObject barsObject = ( BARSObject )anObject;

    // Set some attributes of the new BARS object
    barsObject.setPosition( position );
    barsObject.setOrientation( new Vector3d ( oriX, oriY, oriZ ) );
    barsObject.setIsDistributed( true );
    barsObject.setIsAware( true );
    //barsObject.setProperty( "friendly", "false" );
    newID = objectAndEventManager.generateAndReserveID();
    barsObject.setID( newID );
    BARSObjectIDsCreatedBySAF.add( new Long( newID ) );
    objectAndEventManager.registerObject( barsObject );
    bbPrinter.bbNotice( "JAVA SIDE: Created new BARS object of type " + type + " with ID = " + newID + "\n" );

    return "" + newID;
  }


  /** Change a BARS object -- invoked from native side */
  public synchronized static void changeBARSObject( String ID,
                                       double posX, double posY, double posZ,
                                       double oriX, double oriY, double oriZ,
                                       String propertyName, String propertyValue )
  {
    bbPrinter.bbNotice( "JAVA SIDE: Request to change a BARS object from CPP side: " + ID + ": " +
                        "( " + posX + ", " + posY + ", " + posZ + " ) " +
                        "( " + oriX + ", " + oriY + ", " + oriZ + " ) " +
                        propertyName + ":" + propertyValue + "\n" );

    BARSObject changeMe = ( BARSObject )( objectAndEventManager.findObject( Long.parseLong( ID ) ) );

    if ( changeMe != null )
      {
        Vector3d position = getBARSPositionFromLatlon( new Gdc_Coord_3d( posX, posY, posZ ) );
        
        changeMe.setPositionAndOrientation( position,
                                            new Vector3d ( oriX, oriY, oriZ ) );

        if ( ( propertyName != null ) && ( propertyValue != null ) )
          {
            changeMe.setProperty( propertyName, propertyValue );
          }
      }
    else
      {
        bbPrinter.bbError( "JAVA SIDE: changeBARSObject: can't find object with ID = " + ID + "\n" );
      }
  }


  /** Destroy a BARS object -- invoked from native side */
  public synchronized static void destroyBARSObject( String ID )
  {
    bbPrinter.bbNotice( "JAVA SIDE: Destroy a BARS object from SAF side: " + ID + "\n" );
    
    BARSObject killMe = ( BARSObject )( objectAndEventManager.findObject( Long.parseLong( ID ) ) );
    
    if ( killMe != null )
      {
        killMe.die();
      }
    else
      {
        bbPrinter.bbError( "JAVA SIDE: destroyBARSObject: can't find object with ID = " + ID + "\n" );
      }
    
  }


  public synchronized native void nativeStart( String type );


  public synchronized native void nativeBARSObjectCreated( String type, String ID,
                                              double posX, double posY, double posZ,
                                              double oriX, double oriY, double oriZ );


  public synchronized native void nativeBARSObjectChanged( String ID,
                                                           double posX, double posY, double posZ,
                                                           double oriX, double oriY, double oriZ,
                                                           String propertyName, String propertyValue );


  public synchronized native void nativeBARSObjectDestroyed( String ID );

}


