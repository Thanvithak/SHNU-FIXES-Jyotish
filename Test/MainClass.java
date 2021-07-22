
package jsonparse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class MainClass{
public static void main(String[] args) throws JSONException 
{
    
    Set<String> s= new HashSet<>();
    s.add("header.ver");
    s.add("header.def");
    s.add("details.head");
    s.add("details.king");
 HashMap<String,Set<String>> hmap = new HashMap<>();

         for (String str : s)
         {  
          String[] split = str.split("[.]");
          String  key = split[0];
          String value=split[1];
          Set<String> ss= new HashSet<>();
          ss.add(value);
          if(!hmap.containsKey(key))
          {
              hmap.put(key,ss);
          }
          else
          {
               Set<String> bs= hmap.get(key);
               bs.addAll(ss);
               hmap.put(key,bs);
          }
         }
    System.out.println(hmap);
}

}