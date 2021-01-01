package Project.DataStore;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CRDOperations 
{
	
	    //Creation of new file
		public boolean createFile(String path) throws Exception
		{
		    try
		    {
		    	String fileName = path;
		        File myObj = new File(fileName);
		        
		      //File is created
		        if (myObj.createNewFile()) 
		          return true; 
		        
		        else 
		          throw new Exception("File already exists");
		        
		     }
		    catch (IOException e)
		    {
		        e.printStackTrace();
		        return false;
		    }
		}
		
		//Creation of key-value pair
		public boolean create(String path, String key, String value, int ttl) throws Exception
		{
			//To check whether file is present in the specified path
			File f = new File(path);
			if (!f.exists()) 
				throw new Exception("File does not exists in the sepcified location"); 
			
			//To check whether the file size is within 1GB
			long bytes = f.length();
			if(bytes > 8589934592L)
				throw new Exception("File size can't be greater than 1 GB");
			
			//To check whether key is empty
		    if(key.equals("")) 
	            throw new Exception("FAILED: Key cannot be empty");
		    
		    //To check whether already a key is present in the data store as the specified key
		    //ReadText r = new ReadText();
		    if(f.length()!=0 && dupVal(path, key)==true)
		    	throw new Exception("Key already exists in the datastore");
		    
		    //For channel lock
		    RandomAccessFile file = new RandomAccessFile(path, "rw");
	        FileChannel channel = file.getChannel(); 
	        FileLock lock = null; 
	        
		    try 
		    {
		    	//For getting the creation time 	
		    	Calendar c = Calendar.getInstance();
				Date d = c.getTime();
				
				//To find the expired time by adding time-to-live to created time
		        c.add(Calendar.SECOND, ttl);
		        Date expire = c.getTime();
		        
		        //Creation of key-value pair
		    	JSONParser jsonParser = new JSONParser();
		    	JSONObject obj = new JSONObject();
	    	    obj.put("Key", key);
	    	    JSONObject obj1 = new JSONObject();
	    	    obj1.put("Value", value);
	    	    obj1.put("Time to Live", ttl);
	    	    obj1.put("Created Time", d.toString());
	    	    obj1.put("Expire Time", expire.toString());
	    	    obj.put("Value", obj1);
	    	    
	    	    //When the file is empty
		    	if(f.length()==0)
			    {
			    	JSONArray jsonArray = new JSONArray();
			    	jsonArray.add(obj); 
			    	
			    	//Lock file
			        try
			        { lock = channel.tryLock(); } 
			        catch (final OverlappingFileLockException e)
			        { file.close(); channel.close(); }
			        
			        TimeUnit.SECONDS.sleep(1); 
			        
		    		//Writing the new key-value pair in the file
		            file.writeBytes(jsonArray.toJSONString());
		            return true;
		            //System.out.println("Successfully written");
		            
			    }
		    	
		    	//When the file already has some key-value pairs
		    	else
		    	{
		    		Object ob = jsonParser.parse(new FileReader(path));
		    		JSONArray jsonArray = (JSONArray)ob;
		    		jsonArray.add(obj);
		    		 
		    		//Lock file
			        try { lock = channel.tryLock(); } 
			        catch (final OverlappingFileLockException e) { file.close(); channel.close(); }
			       
			        TimeUnit.SECONDS.sleep(1); 
			        
		    		//Writing the new key-value pair in the file
		            file.writeBytes(jsonArray.toJSONString());
		            return true;
		            //System.out.println("Successfully written");
		            
		    	}            
	        } 
		    
		    catch (Exception e)
		    {
	            e.printStackTrace();
	            return false;
	        }
		    
		    finally
		    {
		    	//Releasing of lock and file closing
		    	if(lock!=null)
		    	{
		    	lock.release(); 
		        file.close(); 
		        channel.close();
		    	}
		        //System.out.println("Successfully RELEASED");
		    }
		}
		
		//To check whether already the specified key is present before writing
		public static boolean dupVal(String path, String key)
		{
			try
			{
				JSONParser jsonParser = new JSONParser();
		        Object ob = jsonParser.parse(new FileReader(path));
		        JSONArray jsonArray = (JSONArray)ob;
		        Iterator<JSONObject> objectIterator =  jsonArray.iterator();
	            while(objectIterator.hasNext()) 
	            {
	                JSONObject object = objectIterator.next();
	                if(key.equals(object.get("Key"))) 
	                {
	                	//Key is present already
	                	return true; 
	                }
	            }
	            
	            //Key not present 
	            return false; 
			}
			
			catch(Exception E)
			{
				System.out.println(E);
			}
			
			//Key not present 
			return false;
		}
		
		//To read the specified key-value pair
		public String read(String path, String key) throws Exception
		{
			//To check whether file is present in the specified path
			File f = new File(path);
			if (!f.exists()) 
				throw new Exception("File does not exists in the sepcified location"); 
			
			//To check whether key is empty
	        if(key.equals("")) 
	        	throw new Exception("FAILED: Key cannot be empty");
	        
	      //To check whether file is empty or not
			if(f.length() == 0)
	    		throw new Exception("File is empty");
	        
	        //For channel lock
	        RandomAccessFile file = new RandomAccessFile(path, "rw");
	        FileChannel channel = file.getChannel(); 
	        FileLock lock = null;
	        
	        try 
	        {    
	        	JSONParser jsonParser = new JSONParser();
	            Object ob = jsonParser.parse(new FileReader(path));
	            JSONArray jsonArray = (JSONArray)ob;
	             
	            //File is locked
		        try 
		        { lock = channel.tryLock(); } 
		        catch (final OverlappingFileLockException e) 
		        { file.close(); channel.close(); }
		        
		        TimeUnit.SECONDS.sleep(1);
		        
	            Iterator<JSONObject> objectIterator =  jsonArray.iterator();
	            while(objectIterator.hasNext()) 
	            {
	                JSONObject object = objectIterator.next();
	                if(key.equals(object.get("Key")))
	                {
	                	JSONObject obj = (JSONObject) object.get("Value");
	                	
	                	//Getting the expire time
	                	String exp = (String) obj.get("Expire Time");
	                	SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy", Locale.ENGLISH);
	                	Date date = formatter.parse(exp);
	                	
	                	//To get the current time
	                	Calendar c = Calendar.getInstance();
	        			Date now = c.getTime();
	        			
	        			//To check whether the key-value has expired
	                	if(now.after(date))
	                    	throw new Exception("Cannot read:Time to live has expired");
	                	
	                	//Output the value of the key specified
	                	String str = "Key          : "+object.get("Key")+"\nValue        : "+obj.get("Value") + "\nCreated Time : "+obj.get("Created Time") + "\nExpire Time  : "+obj.get("Expire Time") + "\nTime to Live : "+obj.get("Time to Live")+" seconds";
	                	return str;
	                }
	            }
			}
	        
	        catch (Exception e) 
	        {
				e.printStackTrace();
				return null;
			}
	        
	        finally
		    {
	        	//Lock released and file closed
		    	lock.release(); 
		        file.close(); 
		        channel.close();
		        //System.out.println("Successfully RELEASED");
		    }
			return null;
		}
		
		//To delete the specified key-value pair
		public boolean delete(String path, String key) throws Exception
		{
			int flag=0;
			
			//To check whether file is empty or not
			File f = new File(path); 
			if(f.length() == 0)
	    		throw new Exception("File is empty");
			
			//To check whether key is empty
			if(key.equals("")) 
				throw new Exception("FAILED: Key cannot be empty");
			
			//For channel lock
			RandomAccessFile file = new RandomAccessFile(path, "rw");
	        FileChannel channel = file.getChannel(); 
	        FileLock lock = null; 
	        
	        try 
	        {
	        	//For the position of the key to be deleted
	        	int pos=-1;
	        	
	        	JSONParser jsonParser = new JSONParser();
	            Object ob = jsonParser.parse(new FileReader(path));
	            JSONArray jsonArray = (JSONArray)ob;
	            
	            //File is locked
		        try
		        { lock = channel.tryLock(); } 
		        catch (final OverlappingFileLockException e)
		        { file.close(); channel.close(); }
		        
		        TimeUnit.SECONDS.sleep(1);
		        
	            Iterator<JSONObject> objectIterator =  jsonArray.iterator();
	            while(objectIterator.hasNext()) 
	            {
	                JSONObject object = objectIterator.next();
	                pos++;
	                if(key.equals(object.get("Key")))
	                {
	                	JSONObject obj = (JSONObject) object.get("Value");
	                	
	                	//Getting the expire time
	                	String exp = (String) obj.get("Expire Time");
	                	SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy", Locale.ENGLISH);
	                	Date date = formatter.parse(exp);
	                	
	                	//To get the current time
	                	Calendar c = Calendar.getInstance();
	        			Date now = c.getTime();
	        			
	        			//To check whether the key-value has expired
	                	if(now.after(date))
	                    	throw new Exception("Cannot read:Time to live has expired");
	                	
	                	//Key is present
	                	flag=1;
	                	break;
	                }
	            }
	            
	            //When key is not present in the data-store
	            if(flag==0)
	            	throw new Exception("Key not present");
	            
	            //Deleting the specified key-value from the data store
	            jsonArray.remove(pos);
	            PrintWriter writer = new PrintWriter(path);
	            writer.print("");
	            file.writeBytes(jsonArray.toJSONString());
				return true;
				//System.out.println("Key successfully deleted");
			}
	        
	        catch (Exception e) 
	        {
				e.printStackTrace();
				return false;
			}
	        
	        finally
		    {
	        	//Lock released and file is closed
		    	lock.release(); 
		        file.close(); 
		        channel.close();
		        //System.out.println("3Successfully RELEASED");
		    }
		}

}
