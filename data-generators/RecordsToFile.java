import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class RecordsToFile {

    private static int getRandomIndex(int size) {
        return new Random().nextInt(size);
    }

    private static double getRandomPrice() {
        return 0.5 + new Random().nextDouble()*(1.8-0.5);
    }

    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Usage: java DataGenerator <output_file> <record_count>");
            return;
        }

        String[] productNames = {"Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"};
        int productNamesSize = productNames.length;

        String outputDir = "../records/";
        String outputFilePath = outputDir+args[0];

        int recordCount = Integer.parseInt(args[1]);

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {

            for (int i=0; i<recordCount; i++) {
                int productNameIndex = getRandomIndex(productNamesSize);
                double price = getRandomPrice();

                String json = String.format("{\"id\": %d, \"name\": \"%s\", \"price\": %.2f}\n", i, productNames[productNameIndex], price);
                writer.write(json);
                writer.flush();
            }
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
            return;
        }
    }
}
