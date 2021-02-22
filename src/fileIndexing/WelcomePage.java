package fileIndexing;
public class WelcomePage {

	private static final String version = "v1.0.0";
	private static final String copyright = "@2019 Arpitha, Manindra, Mingxiao, Nileshwari, Shivani";

	public static void splashScreen() {
		System.out.println(Utility.displayLine("*", 70));
		System.out.println("Welcome to MyDatabase");
		System.out.println("MyDatabase Version " + version);
		System.out.println(copyright);
		System.out.println("\nType \"help;\" to display the supported commands of MyDatabase.\n");
		System.out.println(Utility.displayLine("*", 70));
	}

	public static void displayVersion() {
		System.out.println("MyDatabase Version " + version);
		System.out.println(copyright);
	}
}
