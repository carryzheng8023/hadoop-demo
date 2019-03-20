package xin.carryzheng;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {

        Scanner s = new Scanner(System.in);

        int n = s.nextInt();

        int res = n >> 1;

        System.out.println(res);

    }
}
