package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDhtProvider extends ContentProvider 
{
	/*Declarations Related to DB creation*/
	public static final String TABLE_NAME="ChatMessenger_table";
	static final int DATABASE_VERSION = 1;
	static final String CREATE_TABLE =
			" CREATE TABLE " + TABLE_NAME +
			"( key TEXT, " +

			         	 	" value TEXT);";			//query to create the database table
	public static SQLiteDatabase database;
	static final String DATABASE_NAME = "groupmessenger";			//name of the database table
	static DatabaseHelper dbHelper;
	private static final String VALUE_FIELD = "value";
	
	static final int SERVER_PORT = 10000;
	static String myPort=null;							
	static String myAvdHashVal=null;
	static final String URL = "content://edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider";  //Content URI providor
	static final Uri CONTENT_URI = Uri.parse(URL);
	static int avds_joined=0;					//to keep track of number of AVDs joined
	static int masterNodeFlag=0;
	
	static int no_of_joins=0;
	private static final String KEY_FIELD ="key";

	/*To handle deletion operations*/
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
		int rows=0;
		switch(selection)
		{
		
		case "*": rows= database.delete(TABLE_NAME, null, null);
		break;
		default:
				rows=database.delete(TABLE_NAME,"key=?",new String[]{selection});
				break;

		}
		
		return rows;			//return the number of rows
	}


	public String getType(Uri uri) 
	{

		return null;
	}


	public Uri insert(Uri uri, ContentValues values) 
	{
		
		String values_Key=values.getAsString("key");
		String values_values=values.getAsString("value");
		try
		{

			
		
			if(no_of_joins==1)
			{
				long row = database.insert(TABLE_NAME, "", values);			//insert values in the database table

				if(row >0)							
				{

					Uri newUri = ContentUris.withAppendedId(CONTENT_URI, row);
					getContext().getContentResolver().notifyChange(newUri, null);
					return newUri;
				}
				Log.v("insert", values.toString());
				return ContentUris.withAppendedId(CONTENT_URI, row);

			}
			else
			{
				String hashKeyVal=genHash(values_Key);
				ServerTask serverTask=new ServerTask();
				String successorHash=serverTask.getSuccessor();
				String myPortHashVal=assignPort(myPort);
				String successor_port=serverTask.getSuccessorPort();
				StringBuffer keyValuePair=new StringBuffer();
				//comparing hash values to check conditions
				int key_self=hashKeyVal.compareTo(myPortHashVal);
				int self_successor=myPortHashVal.compareTo(successorHash);
				int hashKeyVal_successor=hashKeyVal.compareTo(successorHash);
				if(ClientTask.successorFlag==true)
				{
					//condition to insert values in successor
					if((key_self>0)&&(hashKeyVal_successor>0)&&(self_successor>=0))
					{
						
						keyValuePair.append(values_Key);
						keyValuePair.append("insert###");
						keyValuePair.append(values_values);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_port);
						keyValuePair.setLength(0);
						return ContentUris.withAppendedId(CONTENT_URI, 0);
					}
					else if((key_self>0)&&(hashKeyVal_successor<0)&&(self_successor<=0))
					{
						
						keyValuePair.append(values_Key);
						keyValuePair.append("insert###");
						keyValuePair.append(values_values);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_port);
						keyValuePair.setLength(0);
						return ContentUris.withAppendedId(CONTENT_URI, 0);
					}
					else if((key_self<0)&&(hashKeyVal_successor<0)&&(self_successor>=0))
					{
						
						keyValuePair.append(values_Key);
						keyValuePair.append("insert###");
						keyValuePair.append(values_values);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_port);
						keyValuePair.setLength(0);
						return ContentUris.withAppendedId(CONTENT_URI, 0);

					}
					//condition to ask successor to forward the value further to check where to insert
					else
					{
						
						keyValuePair.append(values_Key);
						keyValuePair.append("check###");
						keyValuePair.append(values_values);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_port);
						keyValuePair.setLength(0);
						return ContentUris.withAppendedId(CONTENT_URI, 0);
					}
				}
			}
			
			return uri;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		return uri;

	}


	public boolean onCreate()
	{

		getMyPort();
		no_of_joins++;
		Log.v("Number of joine-->",""+no_of_joins);
		try 
		{
			Log.v("inside create",Integer.toString(SERVER_PORT));
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} 

		catch (IOException e)
		{

			Log.e("Error","Can't create a ServerSocket");
			return false;
		}
		try
		{
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, myPort);
		}
		catch (Exception e) 
		{

			e.printStackTrace();
		}
		Context context=getContext();			//initializing the context
		dbHelper = new DatabaseHelper(context);
		database = dbHelper.getWritableDatabase();

		if(database == null)
			return false;
		else
			return true;

	}


	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder)
	{
		Log.v("Number of joins after sever is started-->",""+no_of_joins);
		database = dbHelper.getReadableDatabase();		//Retrieve the database to Query from SQLite Table
		Cursor cursor;
		if(no_of_joins==1)
		{	
			if(selection.equalsIgnoreCase("*") || selection.equalsIgnoreCase("@"))
			{
				cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
				return cursor;
			}
			else
			{

				String query = "SELECT * FROM " + TABLE_NAME + " WHERE  key =? ";
				String[] ary = new String[] { selection };
				cursor=database.rawQuery(query, ary);
				return cursor;
			}
		}
		else
		{
			switch(selection)
			{

			case "*" :
				Log.v("To Retrieve All Values","");
				cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
				StringBuffer getValues=new StringBuffer();
				getValues.append("!!");
				getValues.append(myPort);

				if(cursor.getCount()>0)
				{
					if (cursor.moveToFirst()) 		//iterating the values of self
					{
						do
						{
							int keyIndex = cursor.getColumnIndex(KEY_FIELD);
							int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
							String returnKey = cursor.getString(keyIndex);
							String returnValue = cursor.getString(valueIndex);
							getValues.append("##VS##");
							getValues.append(returnKey);
							getValues.append("==");
							getValues.append(returnValue);

						} while (cursor.moveToNext());
					}

				}
				try
				{
					ServerTask serverTask=new ServerTask();
					String successor_port=serverTask.getSuccessorPort();
					StringBuffer msgToSend=new StringBuffer();
					msgToSend.append(getValues.toString()+"\n");
					//writing on socket values retrieved and sending to successor
					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_port));
					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
					pw.write(msgToSend.toString());  
					pw.flush();
					
					BufferedReader buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
					String line = buff.readLine();
					Log.v("Values returned at Inititator-->","");;
					pw.close(); 
					buff.close();
					skt.close(); 
					String columnNames[]={"key","value"};
					//Creating matrixCursor to store all objects
					MatrixCursor matrixCursor = new MatrixCursor(columnNames);
					String[] a1=line.split("!!");
					String[] s3 =a1[1].split("##VS##");
					//splitting string to retrieve values
					for(int i=1;i<s3.length;i++)
					{
						
						String [] s4=s3[i].split("==");
						matrixCursor.addRow(new String[]{s4[0],s4[1]});

					}
					pw.close(); 
					try
					{
						buff.close();
					} 
					catch (IOException e)
					{

						e.printStackTrace();
					}
					try
					{
						skt.close();
					}
					catch (IOException e)
					{

						e.printStackTrace();
					}
					return matrixCursor;
				} catch (IOException e1) 
				{

					e1.printStackTrace();
				}
				break;
			case "@":Log.v("@ case","handled");
					cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
		
					break;
			default:Log.v("deafault case","");
					cursor=null;
					String hashKeyVal=null;
					Log.v("Query Check","checking in self and others");
					try
					{
						hashKeyVal=genHash(selection);
					}
					catch(Exception e)
					{
						Log.v("Error in genHash","atQuery");
					}
					Log.v("Inserting in the default","sending message to successor");
					ServerTask serverTask=new ServerTask();
					
					String successor_port=serverTask.getSuccessorPort();
					
					String []var={selection};
					cursor=database.query(SimpleDhtProvider.TABLE_NAME, projection, "key=?", var, null, null, sortOrder);
					int val=cursor.getCount();
					Log.v("Count Retrieved-->",""+val);
					if(val>0)
					{
						Log.v("value Found",""+val);
						return cursor;
					}
					else
					{
						Log.v("Call to create socket","Method call");
						String valueRead=createSocketImplement(selection,successor_port);
						if(valueRead==null)
						{
							Log.v("Returned Null-->",valueRead);
							return null;
		
		
						}
						else
						{
							Log.v("Value found",valueRead);
							String columnNames[]={"key","value"};
							MatrixCursor matrixCursor = new MatrixCursor(columnNames);
							matrixCursor.addRow(new String[]{selection,valueRead});
							cursor=matrixCursor;
						}
		
					}
				}
		}	    			

		return cursor;		

	}
	/*To Create Socket Connection to Query */
	public static String createSocketImplement(String selection,String successorPort)
	{
		try
		{
			ServerTask serverTask=new ServerTask();
			String successor_port=serverTask.getSuccessorPort();
			StringBuffer msgToSend=new StringBuffer();
			//Appending Values before Forwarding to successor
			msgToSend.append(myPort);
			msgToSend.append("ToGetVal###");
			msgToSend.append(selection+"\n");
			String Message=msgToSend.toString();
			Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_port));
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
			pw.write(msgToSend.toString());  
			pw.flush();
			//Creating BufferedWriter to read incoming Values
			BufferedReader buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
			String line = buff.readLine();
			pw.close(); 
			buff.close();
			skt.close(); 
			return line;


		}
		catch(Exception e)
		{
			return null;
		}
	}

	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) 
	{

		return 0;
	}

	public String genHash(String input) throws NoSuchAlgorithmException //changed private to public
	{
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) 
		{
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	/*Assign Port Number to various Ports*/
	public String assignPort(String myPort)
	{

		switch(myPort)
		{
		case "11108" : try 
		{
			myAvdHashVal=genHash("5554");
		} 
		catch (NoSuchAlgorithmException e)
		{

			e.printStackTrace();
		}
		break;
		case "11112" : try 
		{
			myAvdHashVal=genHash("5556");
		}
		catch (NoSuchAlgorithmException e)
		{

			e.printStackTrace();
		}
		break;
		case "11116" : try 
		{
			myAvdHashVal=genHash("5558");
		} 
		catch (NoSuchAlgorithmException e)
		{

			e.printStackTrace();
		}
		break;
		case "11120" : try 
		{
			myAvdHashVal=genHash("5560");
		}
		catch (NoSuchAlgorithmException e) 
		{

			e.printStackTrace();
		}
		break;
		case "11124" : try 
		{
			myAvdHashVal=genHash("5562");
		} 
		catch (NoSuchAlgorithmException e) 
		{
			e.printStackTrace();
		}
		break;
		}
		return myAvdHashVal;
	}

	//Assign My Port Value to My Port
	public void getMyPort()
	{
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
	}

	//To Insert Values in Databse after Determing It Belongs
	public Uri insertKey(Uri uri, ContentValues values)
	{
		long row = database.insert(TABLE_NAME, "", values);			//insert values in the database table

		

		Uri newUri = ContentUris.withAppendedId(CONTENT_URI, row);
		getContext().getContentResolver().notifyChange(newUri, null);
		return ContentUris.withAppendedId(CONTENT_URI, 0);
		
	}
	
	/*To Query Database for a Key
	 * 
	 * */
	public static String queryDb(String keyVal)
	{
		try
		{
			database=dbHelper.getReadableDatabase();	
			String query = "Select * FROM " + TABLE_NAME + " WHERE  key =? ";
			String[] ary = new String[] { keyVal };
			Cursor cursor=database.rawQuery(query, ary);
			//If value Value Exists in The Querying Android
			if(cursor.getCount()>0)
			{

				cursor.moveToFirst();
				String value=cursor.getString(1);
				Log.v("Value of the key",value);
				return value;

			}
			else
			{
				return "NotThere";
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}

	}

	/*To retrieve values from My Database
	 * */
	public String getValuesFromMyDb(String readValues) 
	{

		Cursor cursor;
		StringBuffer getValues=new StringBuffer();
		getValues.append(readValues);
		cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
		Log.v("Values Getting inserted are-->","");
		if(cursor.getCount()>0)
		{
			if (cursor.moveToFirst()) 
			{
				do
				{
					//Extracting the Key and Value 
					int keyIndex = cursor.getColumnIndex(KEY_FIELD);
					int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
					String returnKey = cursor.getString(keyIndex);
					String returnValue = cursor.getString(valueIndex);
					getValues.append("##VS##");
					getValues.append(returnKey);
					getValues.append("==");
					getValues.append(returnValue);
					

				} while (cursor.moveToNext());
			}

		}
		return getValues.toString();

	}
}