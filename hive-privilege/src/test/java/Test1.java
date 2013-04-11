import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Test1 {
 
 /**
  * @param args
  * @return 
  * 4
  * 1240
  * 124067
  */
 public static void main(String[] args) {
  String string="1234567";
  Matcher matcher = Pattern.compile("3(4)5").matcher(string);
  if(matcher.find()){
   System.out.println(matcher.group(1));
   StringBuffer sb = new StringBuffer();
   matcher.appendReplacement(sb, matcher.group(1)+"0");  //替换的是整个group()

 //  matcher.appendReplacement(sb, matcher.quoteReplacement("$0"+"$1"+"_recycle"));

   System.out.println(sb);
   matcher.appendTail(sb);
   System.out.println(sb.toString());
   
  }
 }

}

