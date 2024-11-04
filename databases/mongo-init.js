db = db.getSiblingDB('admin');

// Create the root user if it doesn't exist
db.createUser({
    user: "mongo",
    pwd: "mongo",
    roles: [
        { role: "userAdminAnyDatabase", db: "admin" },
        { role: "readWriteAnyDatabase", db: "admin" }
    ]
});
