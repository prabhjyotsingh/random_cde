public class Test {

  public static void main(String[] asd) {

    String oldStr = "a\tbc\\td\\ne";
    StringBuilder newStr = new StringBuilder();

    char[] chrArr = oldStr.toCharArray();
    for (char c : chrArr) {
      if (c == '\n') {
        newStr.append("\\n");
      } else {
        newStr.append(c);
      }
    }

    System.out.println(newStr);
  }
}
