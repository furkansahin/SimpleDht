package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import android.content.ContentUris;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class ServerTask extends AsyncTask<ServerSocket, String, Void>
{
	static String Controller_Port="11108";				//Port number of central AVD
	static String my_Port=SimpleDhtProvider.myPort;			//port number of the avd running
	public static boolean valueRetrievedSet=false;
	static boolean successorFlag=false;		//check if your successor has been set or not	
	static int no_of_connections=0;			//number of nodes in the cycle
	static String mySuccesorAvd=null;		//Hash Value of your successor's port
	SimpleDhtProvider simpleDhtProvider=new SimpleDhtProvider();
	static String portToSend=null;		//port to send the message
	static String successor_portNumber=null; //port Number of your successor
	String minHashVal=null;
	String min_portNumber=null;
	String maxHashVal=null;
	String max_portNumber=null;
	String myPortHashVal=null;
	SimpleDhtProvider simpleDht=new SimpleDhtProvider();
	static final String URL = "content://edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider";  //Content URI providor
	static final Uri CONTENT_URI = Uri.parse(URL);
	Cursor valueCursor;
	Socket socket;
	protected Void doInBackground(ServerSocket... sockets)
	{
		
		try
		{
			myPortHashVal=simpleDht.assignPort(my_Port);
			no_of_connections++;
		}
		catch(Exception e)
		{
			
		}
		while(true)
		{
				
				try
				{
						
						ServerSocket serverSocket = sockets[0];
			            socket=serverSocket.accept();  
			            
						BufferedReader br_Server=new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String read=br_Server.readLine();
						
						
						if(my_Port.equalsIgnoreCase(Controller_Port))
						{
							
							if((!(successorFlag)))
							{
								successorFlag=true;
								ClientTask.successorFlag=true;
								successor_portNumber=Controller_Port;
								no_of_connections++;
								mySuccesorAvd=simpleDht.assignPort(Controller_Port);
							}
							if(read.contains("NewContn"))
							{
								
								SimpleDhtProvider.no_of_joins++;
								String msgReceived[]=read.split("@");
								String portReceived=msgReceived[1];
								String hashPortReceived= simpleDht.assignPort(portReceived);
								StringBuffer toSendMsg=new StringBuffer();
								
								int successorAndSelf=mySuccesorAvd.compareTo(myPortHashVal);
								int compareNewPortVal=hashPortReceived.compareTo(myPortHashVal);
								int compareNewPortWithSuccessor=hashPortReceived.compareTo(mySuccesorAvd);
								
								if(successorAndSelf==0 && (compareNewPortVal!=0))
								{
									
									toSendMsg.append("SetSuccessor");
									toSendMsg.append("@");
									toSendMsg.append(my_Port);
									mySuccesorAvd=hashPortReceived;
									successor_portNumber=portReceived;
									no_of_connections++;
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSendMsg.toString(), portReceived);
								}
							
								else if(((hashPortReceived).compareTo(myPortHashVal)>0) && ((hashPortReceived).compareTo(mySuccesorAvd)>0) &&((mySuccesorAvd).compareTo(myPortHashVal)<=0))
								{
									toSendMsg.append("SetSuccessor");
									toSendMsg.append("@");
									toSendMsg.append(successor_portNumber);
									mySuccesorAvd=hashPortReceived;
									successor_portNumber=portReceived;
									no_of_connections++;
									Log.v("\nChanged Controller's successor as",successor_portNumber);
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSendMsg.toString(), portReceived);
									
								}
								else if(((hashPortReceived).compareTo(myPortHashVal)>0) && ((hashPortReceived).compareTo(mySuccesorAvd)<0) &&((mySuccesorAvd).compareTo(myPortHashVal)>=0))
								{
									
									toSendMsg.append("SetSuccessor");
									toSendMsg.append("@");
									toSendMsg.append(successor_portNumber);
									mySuccesorAvd=hashPortReceived;
									successor_portNumber=portReceived;
									no_of_connections++;
									Log.v("\nChanged Controller's successor as",successor_portNumber);
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSendMsg.toString(), portReceived);
									
								}
								else if(((hashPortReceived).compareTo(myPortHashVal)<0) && ((hashPortReceived).compareTo(mySuccesorAvd)<0) &&((mySuccesorAvd).compareTo(myPortHashVal)<=0))
								{
									toSendMsg.append("SetSuccessor");
									toSendMsg.append("@");
									toSendMsg.append(successor_portNumber);
									mySuccesorAvd=hashPortReceived;
									successor_portNumber=portReceived;
									no_of_connections++;
									Log.v("\nChanged Controller's successor as",successor_portNumber);
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSendMsg.toString(), portReceived);
									
								}
								else
								{
									toSendMsg.append("CheckSuccessor");
									toSendMsg.append("@");
									toSendMsg.append(portReceived);
									no_of_connections++;
									new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSendMsg.toString(), successor_portNumber);
									
								}
		
							}
							//Retreive All values
							else if(read.contains("!!"))
							{
								retrieveAllValues(read);
							}
							else if(read.contains("insert###"))
							{
							
							ContentValues keyValueToInsert=new ContentValues();
							String splitMsgString[]=read.split("insert###");
							
							String key=splitMsgString[0];
							String value=splitMsgString[1];
							keyValueToInsert.put("value",value);
							keyValueToInsert.put("key",key);
							Uri uri_=simpleDht.insertKey(CONTENT_URI, keyValueToInsert);
							
				
							}
							//Check to Forward of Insert
							else if(read.contains("check###"))
							{
							
									checkToForward(read);
															
							}
							//Check to Retreieve Values
							else if(read.contains("getValue###"))
							{
								StringBuffer queryVal=new StringBuffer();
								String queryValPort[]=read.split("getValue###");
								String portToSend=queryValPort[0];
								String keyToLook=queryValPort[1];
								//Query to Retrieve Values
								String value=SimpleDhtProvider.queryDb(keyToLook);
								
								if(value==null)
								{
									
									StringBuffer msgToSend=new StringBuffer();
									msgToSend.append(my_Port);
									msgToSend.append("ToGetVal###");
									msgToSend.append(keyToLook+"\n");
									String Message=msgToSend.toString();
									Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_portNumber));
									PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
									pw.write(msgToSend.toString());  
									msgToSend.setLength(0);
									pw.flush();
									
									
									BufferedReader bufferRead= new BufferedReader(new InputStreamReader(skt.getInputStream()));
									String val_found = bufferRead.readLine();
									PrintWriter writer = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
									writer.write(val_found);  //write the message to output stream
									writer.flush();
									writer.close();
									pw.close(); 
									bufferRead.close();
									skt.close(); 
									
								}
								else
								{
									
								
									String valueReceived=value+"\n";
									PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
								    out.write(valueReceived);
								    out.flush();
								    out.close();
									
								}
											
							 }
							else if(read.contains("ToGetVal###"))
							{
								
								StringBuffer queryVal=new StringBuffer();
								String queryValPort[]=read.split("ToGetVal###");
								String portToSend=queryValPort[0];
								String keyToLook=queryValPort[1];
								
								String value=SimpleDhtProvider.queryDb(keyToLook);
								
								if(value.equalsIgnoreCase("NotThere"))
								{
									
									StringBuffer msgToSend=new StringBuffer();
									msgToSend.append(my_Port);
									msgToSend.append("ToGetVal###");
									msgToSend.append(keyToLook+"\n");
									//Sending Message through PrintWriter
									Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_portNumber));
									PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
									pw.write(msgToSend.toString());  
									msgToSend.setLength(0);
									pw.flush();								
									BufferedReader bufferRead= new BufferedReader(new InputStreamReader(skt.getInputStream()));
									String val_found = bufferRead.readLine();
									PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
									StringBuffer valueReturned=new StringBuffer();
									valueReturned.append(val_found);
									valueReturned.append("\n");
									writer.print(valueReturned.toString());  
									writer.flush();
									
									pw.close(); 
									bufferRead.close();
									writer.close();
									skt.close();
									
								}
								else
								{
									StringBuffer valueToSend=new StringBuffer();
									valueToSend.append(value);
									valueToSend.append("\n");
									PrintWriter out=new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
								    out.print(valueToSend.toString());
								    out.flush();
								    out.close();
								}
							}
							else if(read.contains("valueRetrieved###"))
							{
								String getValueSent[]=read.split("valueRetrieved###");
								String keyRetrieved=getValueSent[0];
								String valueRetrieved=getValueSent[0];
							
								String columnNames[]={"key","value"};
								MatrixCursor matrixCursor = new MatrixCursor(columnNames);
								matrixCursor.addRow(new String[]{keyRetrieved,valueRetrieved});
								valueCursor=matrixCursor;
								valueRetrievedSet=true;
								
							}

							
						}
						else
						{
							if(read.contains("@"))
							{
								SimpleDhtProvider.no_of_joins++;
								//SimpleDhtProvider.no_of_joins++;
								String msgReceived[]=read.split("@");
								String portToCheck=msgReceived[1];
								String hashPortToCheck=simpleDht.assignPort(portToCheck);
								int count=0;
								if(read.contains("SetSuccessor"))
								{
									successor_portNumber=msgReceived[1];
									mySuccesorAvd=simpleDht.assignPort(successor_portNumber);
									no_of_connections++;
									successorFlag=true;
								}
								else
								{
									
									if(read.contains("CheckSuccessor"))
									{
										

										if(((hashPortToCheck).compareTo(mySuccesorAvd)<0) && ((hashPortToCheck).compareTo(myPortHashVal)>0) && ((myPortHashVal).compareTo(mySuccesorAvd)<=0))
										{
											StringBuffer newMsgToSend=new StringBuffer();
											newMsgToSend.append("SetSuccessor");
											newMsgToSend.append("@");
											newMsgToSend.append(successor_portNumber);
											successor_portNumber=portToCheck;
											mySuccesorAvd=simpleDht.assignPort(successor_portNumber);
											no_of_connections++;											
											new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsgToSend.toString(),portToCheck);

										}
										else if(((hashPortToCheck).compareTo(mySuccesorAvd)>0) && ((hashPortToCheck).compareTo(myPortHashVal)>0) && ((myPortHashVal).compareTo(mySuccesorAvd)>=0))
										{
											StringBuffer newMsgToSend=new StringBuffer();
												newMsgToSend.append("SetSuccessor");
												newMsgToSend.append("@");
												newMsgToSend.append(successor_portNumber);
												successor_portNumber=portToCheck;
												mySuccesorAvd=simpleDht.assignPort(successor_portNumber);
												no_of_connections++;
												new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsgToSend.toString(),portToCheck);

										}
										else if(((hashPortToCheck).compareTo(mySuccesorAvd)<0) &&((hashPortToCheck).compareTo(myPortHashVal)<0) &&((myPortHashVal).compareTo(mySuccesorAvd)>=0))
										{
											StringBuffer newMsgToSend=new StringBuffer();
											newMsgToSend.append("SetSuccessor");
											newMsgToSend.append("@");
											newMsgToSend.append(successor_portNumber);
											successor_portNumber=portToCheck;
											mySuccesorAvd=simpleDht.assignPort(successor_portNumber);
											no_of_connections++;
											if(count==0)
											{
												new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsgToSend.toString(),portToCheck);
												if(my_Port.equalsIgnoreCase("11120"))
												{
													count++;
												}
											}
											Log.v("Successor Set as-->",successor_portNumber);
											
										}
										else
										{
											
											StringBuffer newMsgToSend=new StringBuffer();
											newMsgToSend.append("CheckSuccessor");
											newMsgToSend.append("@");
											newMsgToSend.append(portToCheck);
											new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsgToSend.toString(), successor_portNumber);
										}
									}
								
									
								}
								
								
							}
							else if(read.contains("###"))
							{
								//inserting the values in Database
								if(read.contains("insert###"))
								{
								
									ContentValues keyValueToInsert=new ContentValues();
									String splitMsgString[]=read.split("insert###");
								
									String key=splitMsgString[0];
									String value=splitMsgString[1];
									keyValueToInsert.put("value",value);
									keyValueToInsert.put("key",key);
									Uri uri=simpleDht.insertKey(CONTENT_URI, keyValueToInsert);
									
								}
								//To Check if Value is to be kept or Forwarded
							else if(read.contains("check###"))
							{
								checkToForward(read);
							}
							
						//To check if the Key is Present in Others
							else if(read.contains("ToGetVal###"))
							{
								
								StringBuffer queryVal=new StringBuffer();
								String queryValPort[]=read.split("ToGetVal###");
								String portToSend=queryValPort[0];
								String keyToLook=queryValPort[1];
								
								String value=SimpleDhtProvider.queryDb(keyToLook);
								
								if(value.equalsIgnoreCase("NotThere"))
								{
									
									StringBuffer msgToSend=new StringBuffer();
									msgToSend.append(my_Port);
									msgToSend.append("ToGetVal###");
									msgToSend.append(keyToLook+"\n");
									String Message=msgToSend.toString();
									Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_portNumber));
									PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
									pw.write(msgToSend.toString());  
									msgToSend.setLength(0);
									pw.flush();							
									
									boolean val=pw.checkError();

									BufferedReader bufferRead= new BufferedReader(new InputStreamReader(skt.getInputStream()));
									String val_found = bufferRead.readLine();
									PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
									StringBuffer valueReturned=new StringBuffer();
									valueReturned.append(val_found);
									valueReturned.append("\n");
									writer.print(valueReturned.toString()); 
									writer.flush();
									
									pw.close(); 
									bufferRead.close();
									writer.close();
									skt.close();
									
								}
								else
								{
									
									StringBuffer valueToSend=new StringBuffer();
									valueToSend.append(value);
									valueToSend.append("\n");
									PrintWriter out=new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
								    out.print(valueToSend.toString());
								    out.flush();
								    out.close();
								}
							}
				
							}
							else if(read.contains("!!"))
							{
								retrieveAllValues(read);
							}
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
		
		
			}
			
	}
	/*To Send And Retreieve All Values
	 * */
	private void retrieveAllValues(String read) throws IOException
	{
		String []putValues=read.split("##VS##");
		String []portNo=putValues[0].split("!!");
		if(portNo[1].equalsIgnoreCase(successor_portNumber))
		{
			
			Log.v("My successor is inititator-->","Resend All values");
			String valuesRetrieved=simpleDht.getValuesFromMyDb(read);    
			PrintWriter writer = null;
			try
			{
				writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
			} 
			catch (IOException e)
			{
				
				e.printStackTrace();
			}
			StringBuffer valueReturned=new StringBuffer();
			valueReturned.append(valuesRetrieved);
			valueReturned.append("\n");
			writer.print(valueReturned.toString());  
			 //write the message to output stream
			writer.flush();
			writer.close();
			
		}
		else
		{
			String valuesRetrieved=simpleDht.getValuesFromMyDb(read);
			Socket skt = null;
			try 
			{
				skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_portNumber));
			} 
			catch (NumberFormatException | IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
			pw.write(valuesRetrieved+"\n");  
			
			pw.flush();			
			
			boolean val=pw.checkError();

		
			BufferedReader bufferRead= new BufferedReader(new InputStreamReader(skt.getInputStream()));
			String valToReturn = null;
			try 
			{
				valToReturn = bufferRead.readLine();
			} 
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			PrintWriter writer = null;
			try 
			{
				writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			StringBuffer valueReturned=new StringBuffer();
			valueReturned.append(valToReturn);
			valueReturned.append("\n");
			writer.print(valueReturned.toString());  
			//write the message to output stream
			writer.flush();
			pw.close(); 
			try {
				bufferRead.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer.close();
			try {
				skt.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
		
	}

	/*Getter to Get Successor Hash Values
	 * */
	public String getSuccessor()
	{
		return mySuccesorAvd;
	}
	
	/*Getter to get successor Port Value*/
	public String getSuccessorPort()
	{
		return successor_portNumber;
	}
	
	
	/*Check to Forward where to send the Value Sent by Predecessor
	 * */
	public void checkToForward( String read)
	{
		String splitMsgString[]=read.split("check###");
		String values_Key=splitMsgString[0];
		String values_values=splitMsgString[1];
		String hashVal=null;
		String hashKeyVal=null;
		
		StringBuffer keyValuePair=new StringBuffer();
		try
		{
			hashVal=simpleDht.genHash(values_values);
			hashKeyVal=simpleDht.genHash(values_Key);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		//Hashing the Values Before Compare
		int key_self=hashKeyVal.compareTo(myPortHashVal);
		int self_successor=myPortHashVal.compareTo(mySuccesorAvd);
		int hashKeyVal_successor=hashKeyVal.compareTo(mySuccesorAvd);
		
		//Checking Various Conditions to Insert Values in DB or forward to successor
		if((key_self>0)&&(hashKeyVal_successor>0)&&(self_successor>=0))
		{
			
			keyValuePair.append(values_Key);
    		keyValuePair.append("insert###");
    		keyValuePair.append(values_values);
    		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_portNumber);
			keyValuePair.setLength(0);
		}
		else if((key_self>0)&&(hashKeyVal_successor<0)&&(self_successor<=0))
		{
			
			keyValuePair.append(values_Key);
    		keyValuePair.append("insert###");
    		keyValuePair.append(values_values);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_portNumber);
			keyValuePair.setLength(0);
		}
		else if((key_self<0)&&(hashKeyVal_successor<0)&&(self_successor>=0))
		{
			
			keyValuePair.append(values_Key);
    		keyValuePair.append("insert###");
    		keyValuePair.append(values_values);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_portNumber);
			keyValuePair.setLength(0);
			
		}
		else
		{
			keyValuePair.append(values_Key);
			keyValuePair.append("check###");
    		keyValuePair.append(values_values);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValuePair.toString() ,successor_portNumber);
			keyValuePair.setLength(0);
		}
	}
		
	

	

}