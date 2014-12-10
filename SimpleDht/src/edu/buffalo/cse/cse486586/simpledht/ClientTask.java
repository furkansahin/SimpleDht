package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import android.os.AsyncTask;
import android.util.Log;

public class ClientTask extends AsyncTask<String, Void, Void>
{
	static final String[] REMOTE_PORT = {"5554","5556","5558","5560","5562"};
	SimpleDhtProvider simpleProvider= new SimpleDhtProvider();
	String Hash_Remote_Port[];
	String Controller_Port="11108";
	static String my_Port=SimpleDhtProvider.myPort;
	static boolean successorFlag=false;
	SimpleDhtProvider simpleDhtProvider=new SimpleDhtProvider();
	public int count=0;
	protected Void doInBackground(String... msgs) 
	{
		
			try
			{
				
				Socket clientSide=null;
				StringBuffer msgToSend=new StringBuffer();
				if(!(my_Port).equalsIgnoreCase(Controller_Port))		//sending by all except 
				{ 
					/*Initial Message Sent to Controller stating the new AVD has joined*/
					if((successorFlag==false) && count==0)
					{
						count++;
						Log.v("count Val-->",""+count);
						msgToSend.append("NewContn");
						msgToSend.append("@");
						msgToSend.append(my_Port);
						Log.v("\nMessage Sent is-->",msgToSend.toString());
						successorFlag=true;
						Log.v("successor flag is set as-->",""+successorFlag);
						clientSide=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(Controller_Port));
						
						
					}
					/*Sending Message while re-assigning its successor*/
					else
					{
						
						Log.v("Msg sent to successor", msgs[1]);
						clientSide=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(msgs[1]));
						msgToSend.append(msgs[0]);
							
					}
				}
				else 
				{
					
					Log.v("Port Number being sent",msgs[1]);
					Log.v("Being sent by Port",my_Port);
					Log.v("Message Being sent is",msgs[0]);
					
					clientSide=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(msgs[1]));
					msgToSend.append(msgs[0]);
					
					
				}
			
			if(clientSide.isConnected())
			{
				BufferedWriter new_Client=new BufferedWriter(new OutputStreamWriter(clientSide.getOutputStream()));
				//Writing the client message
				new_Client.write(msgToSend.toString());
				new_Client.flush();
				msgToSend.setLength(0);
				//Closing the Buffered Writer
				new_Client.close();
			}
			/*Code Changes End*/
			clientSide.close();
			
			}
			catch(Exception e)
			{
				Log.e("Exception-->",my_Port);
				Log.e("ClientSide Exception at",""+my_Port+"Exception thrown");
				e.printStackTrace();
			
			}
	
		return null;

	}
}
