package cn.com.boe.itc.testUtils;

import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

public class Timetest {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String str1="2020-01-01 06:19:55";
        String str2="2020-01-01 06:19:55";
        int res=str1.compareTo(str2);
        if(res>0)
            System.out.println("str1>str2");
        else if(res==0)
            System.out.println("str1=str2");
        else
            System.out.println("str1<str2");
    }

    @Test
    public void testSet(){
        Set<String> s1 = new LinkedHashSet<>();

        s1.add("5");
        s1.add("5");
        s1.add("100000");
        s1.add("1");
        s1.add("4");
        s1.add("4");
        s1.add("2");
        s1.add("A");
        s1.add("3");
        s1.add("a");
        s1.add("----a");
        System.out.println("s1 = " + s1);
        for (String tem1 :
                s1) {
            System.out.print(tem1+", ");
        }

    }


}