#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int getRandomIndex(int size) {
    return rand()%size;
}

double getRandomPrice() {
    return 0.5 + ((double)rand()/RAND_MAX) * (1.8-0.5);
}

int main(int argc, char* argv[]) {
    if (argc!=3) {
        printf("Usage: %s <output_file> <record_count>\n", argv[0]);
        return 1;
    }

    const char* productNames[] = {"Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"};
    int productNamesSize = sizeof(productNames)/sizeof(productNames[0]);

    const char* output_dir = "../records/";
    char output_file_path[100];
    snprintf(output_file_path, sizeof(output_file_path), "%s%s", output_dir, argv[1]);

    int record_count = atoi(argv[2]);

    srand(time(NULL));

    FILE* file = fopen(output_file_path, "w");
    if (!file) {
        printf("Error opening file: %s\n", output_file_path);
        return 1;
    }

    for (int i=0; i<record_count; i++) {
        int productNameIndex = getRandomIndex(productNamesSize);
        double price = getRandomPrice();

        char jsonBuffer[128];
        snprintf(jsonBuffer, sizeof(jsonBuffer), "{\"id\": %d, \"name\": \"%s\", \"price\": %.2f}\n", i, productNames[productNameIndex], price);

        fputs(jsonBuffer, file);
        fflush(file);
    }
    fclose(file);

    return 0;
}
