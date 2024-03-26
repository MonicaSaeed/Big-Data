package question_2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FriendsReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String, Set<String>> friendsMap = new HashMap<>();
        for (Text val : values) {
            String[] row = val.toString().split(",");
            if (row.length == 2) {
                String person1 = row[0].trim();
                String person2 = row[1].trim();
                if (!friendsMap.containsKey(person1)) {
                	Set<String> s1 =  new HashSet<>();
                    friendsMap.put(person1, s1);
                }
                if (!friendsMap.containsKey(person2)) {
                	Set<String> s2 =  new HashSet<>();
                    friendsMap.put(person2, s2);
                }
                if(person1.contains(person2)){
                	continue;
                }
                friendsMap.get(person1).add(person2);
                friendsMap.get(person2).add(person1);
            }
        }
        
        Set<String> friendsList = friendsMap.get(key.toString());
        Set<String> fofSet = new HashSet<>();

        for (String friend : friendsList) {
            Set<String> theirFriends = friendsMap.get(friend);
            if (theirFriends != null) { // Check if the friend's list is not null
                fofSet.addAll(theirFriends);
            }
        }

        fofSet.remove(key.toString()); // Remove the person itself

        StringBuilder sb = new StringBuilder();
        for (String fof : fofSet) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(fof);
        }

        context.write(new Text(key), new Text(sb.toString()));
    }
}

