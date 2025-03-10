
const fs = require("fs");

// MongoDB database name
const DB_NAME = "ecommerce";
const DATASET_PATH = "C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/json_files";
const BATCH_SIZE = 5000;  //  Insert in smaller chunks

// Collections and JSON file mappings
const FILES = {
    campaigns: "campaigns",
    client_first_purchase: "client_first_purchase",
    events: "events",
    friends: "friends",
    messages: "messages"
};

// Connect to MongoDB
const db = connect("mongodb://localhost:27017/" + DB_NAME);

// Drop database before inserting new data
db.dropDatabase();
print(`Database ${DB_NAME} recreated.`);

// Function to insert data in batches
function insertInBatches(collection, data) {
    let totalInserted = 0;
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
        let batch = data.slice(i, i + BATCH_SIZE);
        let result = db[collection].insertMany(batch);
        totalInserted += result.insertedCount;
        print(`Inserted ${result.insertedCount} records into ${collection} (Batch ${Math.floor(i / BATCH_SIZE) + 1})`);
    }
    return totalInserted;
}

// Function to load and insert JSON data from multiple files
function loadAndInsertJSONFiles(collection, baseFilename) {
    let totalInserted = 0;
    let fileIndex = 1;

    while (true) {
        let filePath = `${DATASET_PATH}/${baseFilename}_${fileIndex}.json`;
        if (!fs.existsSync(filePath)) break;  // Stop if file doesn't exist

        print(`Importing ${filePath} into ${collection}...`);

        try {
            let fileContent = JSON.parse(fs.readFileSync(filePath, "utf8"));
            let inserted = insertInBatches(collection, fileContent);
            totalInserted += inserted;
        } catch (err) {
            print(`Error reading JSON file: ${filePath} - ${err.message}`);
            return;
        }

        fileIndex++;
    }

    print(`Finished inserting ${totalInserted} records into ${collection}`);
}

// Load JSON files and insert them into MongoDB
for (let collection in FILES) {
    const baseFilename = FILES[collection];

    if (fs.existsSync(`${DATASET_PATH}/${baseFilename}.json`)) {
        print(`Importing ${DATASET_PATH}/${baseFilename}.json into ${collection}...`);
        try {
            let fileContent = JSON.parse(fs.readFileSync(`${DATASET_PATH}/${baseFilename}.json`, "utf8"));
            let totalInserted = insertInBatches(collection, fileContent);
            print(`Finished inserting ${totalInserted} records into ${collection}`);
        } catch (err) {
            print(`Error inserting into ${collection}: ${err.message}`);
        }
    } else {
        // Handle split files (_1.json, _2.json, etc.)
        loadAndInsertJSONFiles(collection, baseFilename);
    }
}

print("🎉 All data successfully uploaded to MongoDB!");
