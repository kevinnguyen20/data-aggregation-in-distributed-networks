package org.inet.flink.model;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.Random;
import java.nio.file.Paths;
import java.io.Serializable;

public class Delay implements Serializable {
    private double minDelay;
    private double avgDelay;
    private double maxDelay;
    private double mdevDelay;

    public Delay(int lineIndex) {
        try (BufferedReader reader = new BufferedReader(new FileReader(Paths.get("").toAbsolutePath().toString()+"/delays.txt"))) {
            String line;
            // Skip the header line
            // reader.readLine();
    
            // Read the lines and extract the delay data
            int currentLine = 0;
            while ((line = reader.readLine()) != null) {
                if (currentLine == lineIndex) {
                    String[] parts = line.split(", ");
                    this.minDelay = Double.parseDouble(parts[2]);
                    this.avgDelay = Double.parseDouble(parts[3]);
                    this.maxDelay = Double.parseDouble(parts[4]);
                    this.mdevDelay = Double.parseDouble(parts[5]);
    
                    // Only read the line with the specified index
                    break;
                }
                currentLine++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // Delay in ms
    public double calculateDelay() {
        Random random = new Random();
        double probability = 1.0 / 5.0;
        if (random.nextDouble()<probability) {
            double gaussian = random.nextGaussian() * this.mdevDelay + this.avgDelay;
            double delay = Math.min(this.maxDelay, Math.max(this.minDelay, gaussian));
            System.out.println("Delay: " + delay / 2);
            return delay / 2.0;
        } else {
            // No delay with 4/5 probability
            return 0.0;
        }
    }
    
    public double getMinDelay() {
        return minDelay;
    }

    public double getAvgDelay() {
        return avgDelay;
    }

    public double getMaxDelay() {
        return maxDelay;
    }

    public double getMdevDelay() {
        return mdevDelay;
    }
}
