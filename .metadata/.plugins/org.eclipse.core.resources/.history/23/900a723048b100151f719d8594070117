package mapreduce;

import java.util.Scanner;

public class Main {

	private static Scanner reader;

	public static void main(String[] args) {
		reader = new Scanner(System.in);
		int select = 0;

		System.out.println("Choose one of them");
		System.out.println("  1. Avarage dollar obligated amount");
		System.out.println("  2. Contract action type amount");
		System.out.println("  3. Standart Deviation of dollars Obligated amount");
		System.out.println("  4. IndexColumnList");
		select = reader.nextInt();

		switch (select) {
		case 1:
			try {
				AvarageDollarsObligated.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case 2:
			try {
				ContractActionType.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case 3:
			try {
				StandartDeviationOfDollarsObligated.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case 4:
			try {
				IndexColumnList.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		default:
			System.out.println("Wrong select! Try Again!");
			break;
		}
		
	}

}
