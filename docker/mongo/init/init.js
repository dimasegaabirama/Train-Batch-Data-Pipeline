// Gunakan API fs dari JavaScript untuk membaca file
const fs = require('fs');

db = db.getSiblingDB('train_ticket_db');

print("Initializing MongoDB with train ticket data...");

// === Helper function ===
function importData(collectionName, filePath) {
  try {
    print(`Importing ${collectionName} from ${filePath}...`);

    const jsonText = fs.readFileSync(filePath, 'utf8');
    const docs = JSON.parse(jsonText);

    if (Array.isArray(docs) && docs.length > 0) {
      db[collectionName].insertMany(docs);
      print(`Successfully imported ${docs.length} documents into ${collectionName}`);
    } else {
      print(`No documents found in ${filePath}`);
    }
  } catch (err) {
    print(`Failed to import ${collectionName}: ${err.message}`);
  }
}

// === Import all collections ===
importData('stations', '/docker-entrypoint-initdb.d/data/stations.json');
importData('trains', '/docker-entrypoint-initdb.d/data/trains.json');
importData('routes', '/docker-entrypoint-initdb.d/data/routes.json');
importData('passengers', '/docker-entrypoint-initdb.d/data/passengers.json');
importData('tickets', '/docker-entrypoint-initdb.d/data/tickets.json');

print("MongoDB train ticket database initialized successfully!");
