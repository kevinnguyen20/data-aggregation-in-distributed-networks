#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>
#include <cjson/cJSON.h>

#define MAX_PRODUCT_NAME_LEN 11
#define JSON_BUFFER_SIZE 256

static volatile sig_atomic_t run = 1;

// Signal termination of the program (i.e. Crtl-C)
static void stop(int sig) {
    run = 0;
}

// Struct for product information
typedef struct {
    int id;
    char name[MAX_PRODUCT_NAME_LEN];
    double price;
} Product;

// Function to generate a random product
Product generate_product(int id) {
    Product product;
    product.id = id;
    char* product_names[] = {"Apple", "Banana", "Lemon", "Cherry", "Melon", "Peach", "Grapefruit"};
    strncpy(product.name, product_names[rand() % (sizeof(product_names) / sizeof(product_names[0]))], MAX_PRODUCT_NAME_LEN - 1);
    product.name[MAX_PRODUCT_NAME_LEN - 1] = '\0';
    product.price = (double)rand() / RAND_MAX * (1.8 - 0.5) + 0.5;
    return product;
}

char* generate_product_json(const Product* product) {
    cJSON* root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "id", product->id);
    cJSON_AddStringToObject(root, "name", product->name);
    cJSON_AddNumberToObject(root, "price", product->price);

    char* json_str = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);

    return json_str;
}

void extract_data(int line_index, char* lines[], float* min_delay, float* avg_delay, float* max_delay, float* mdev_delay) {
    char* line = lines[line_index];
    // Parse the line into parts
    char* parts[6];
    char* token = strtok(line, ", ");
    int i = 0;
    while (token != NULL) {
        parts[i++] = token;
        token = strtok(NULL, ", ");
    }
    *min_delay = atof(parts[2]);
    *avg_delay = atof(parts[3]);
    *max_delay = atof(parts[4]);
    *mdev_delay = atof(parts[5]);
}

void generate_data(const char* kafka_bootstrap_servers, const char* topic, int line_index, int number_of_cluster) {
    rd_kafka_t* producer;
    rd_kafka_conf_t* conf;
    rd_kafka_conf_res_t res;
    char errstr[512];

    // Configuration object
    conf = rd_kafka_conf_new();

    res = rd_kafka_conf_set(conf, "bootstrap.servers", kafka_bootstrap_servers, errstr, sizeof(errstr));
    if (res!=RD_KAFKA_CONF_OK)
        fprintf(stderr, "%s\n", errstr);

    res = rd_kafka_conf_set(conf, "compression.codec", "gzip", errstr, sizeof(errstr));
    if (res!=RD_KAFKA_CONF_OK)
        fprintf(stderr, "%s\n", errstr);

    res = rd_kafka_conf_set(conf, "batch.size", "16384", errstr, sizeof(errstr));
    if (res!=RD_KAFKA_CONF_OK)
        fprintf(stderr, "%s\n", errstr);

    res = rd_kafka_conf_set(conf, "linger.ms", "5", errstr, sizeof(errstr));
    if (res!=RD_KAFKA_CONF_OK)
        fprintf(stderr, "%s\n", errstr);

    // Create the Kafka producer instance
    producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
        rd_kafka_conf_destroy(conf);
        fprintf(stderr, "Failed to create Kafka producer: %s\n", errstr);
        return;
    }

    // Configuration object is now owned by the producer
    conf = NULL;

    // Loop to produce data
    int i = 1;
    // int messages_sent = 0;
    // time_t start_time = time(NULL);
    // srand(start_time); // Seed the random number generator
    while (run) {
        Product product = generate_product(i);
        char* json_record = generate_product_json(&product);

        // Produce the product to Kafka
        rd_kafka_produce(
            rd_kafka_topic_new(producer, topic, NULL),
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            json_record,
            strlen(json_record),
            NULL, 0, NULL
        );

        free(json_record);

        i++;
        // messages_sent++;

        // Check if 10 seconds have passed
        // time_t current_time = time(NULL);
        // if (current_time - start_time >= 10) {
        //     double elapsed_time = (double)(current_time - start_time);
        //     double throughput = (double)messages_sent / elapsed_time;
        //     printf("Throughput: %.2f messages per second\n", throughput);

        //     // Reset counters
        //     messages_sent = 0;
        //     start_time = current_time;
        // }
    }

    // Flush any outstanding messages
    rd_kafka_flush(producer, 10 * 1000 /* wait for max 10 seconds */);

    // Destroy the Kafka producer
    rd_kafka_destroy(producer);
}

int main(int argc, char* argv[]) {
    if (argc!=3) {
        fprintf(stderr, "Usage: %s <number_of_cluster> <line_index>\n", argv[0]);
        return 1;
    }

    int number_of_cluster = atoi(argv[1]);
    int line_index = atoi(argv[2]);

    // Validate the number of clusters
    if (number_of_cluster<1 || number_of_cluster>2) {
        fprintf(stderr, "Invalid number of cluster. Please choose between 1 and 2\n");
        return 1;
    }

    // Change the working directory to the script's directory
    // if (chdir("./data-generators/")!=0) {
    //     fprintf(stderr, "Failed to change the working directory\n");
    //     return 1;
    // }

    // Validate the line index
    FILE* file = fopen("delays.txt", "r");
    if (file==NULL) {
        fprintf(stderr, "Failed to open 'delays.txt'\n");
        return 1;
    }

    char buffer[256];
    int line_count = 0;
    while (fgets(buffer, sizeof(buffer), file)) {
        line_count++;
    }

    fclose(file);

    if (line_index<1 || line_index>=line_count) {
        fprintf(stderr, "Invalid line index. Please choose a number between 1 and %d\n", line_count-1);
        return 1;
    }

    const char* kafka_bootstrap_servers = "kafka:29092";
    const char* producer_topic = (number_of_cluster == 1) ? "flink-kafka-topic" : "flink-kafka-topic-2";

    generate_data(kafka_bootstrap_servers, producer_topic, line_index, number_of_cluster);

    return 0;
}
