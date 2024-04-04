package Q2;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class FriendshipReducer extends Reducer<Text, Text, Text, Text> {
	
	public Set<String> friends=new HashSet<>();
	public Set<String> friendsOfFriends=new HashSet<>();

	 @Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// int flag;

		 if(key.toString().trim().equals("P1".trim()))
		 {
			 for(Text value:values)
			 {
				 friends.add(value.toString());
				 
			 }
		 }
	        
		// flag=0;
		 if(!friends.isEmpty() && friends.contains(key.toString().trim()))
		 {
			 
			for(Text value:values) 
			{
				if(!friendsOfFriends.contains(value) && value!=key && !value.toString().equals("P1"))
				{
					
				friendsOfFriends.add(value.toString());
				//flag=1; 	//	friends.remove(key.toString().trim());

				}
				
			}
		 
	 }
		// if(friends.isEmpty())
		//	{
				
			Text friends_of_friends=new Text();
			friends_of_friends.set(friendsOfFriends.toString());
			//key.set("P1");
			context.write(key, friends_of_friends);}
		// }
	 
}
	 

