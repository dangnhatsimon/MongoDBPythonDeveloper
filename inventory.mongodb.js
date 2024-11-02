const fs = require("fs");

const database = "inventory";
const collection = "products";

use(database);

db.products.drop();

db.createCollection(collection);

const productsData = [
  {
    name: "Smartphone",
    price: 499.99,
    category: "Electronics",
    description: "A high-end smartphone with",
  },
  {
    name: "Laptop",
    price: 899.95,
    category: "Electronics",
    description: "A powerfull laptop",
  },
  {
    name: "Running Shoes",
    price: 89.99,
    category: "Sports",
    description: "Comfortable and lightweight",
  },
  {
    name: "Wireless Earbuds",
    price: 79.99,
    category: "Electronics",
    description: "True wireless earbuds",
  },
  {
    name: "Dress Shirt",
    price: 49.95,
    category: "Fashion",
    description: "A stylish dress shirt",
  },
  {
    name: "Yoga Mat",
    price: 29.99,
    category: "Sports",
    description: "A non-slip yoga mat for comfortable",
  },
  {
    name: "Coffee Maker",
    price: 69.95,
    category: "Appliances",
    description: "A programmable coffee maker",
  },
  {
    name: "Digital Camera",
    price: 349.0,
    category: "Electronics",
    description: "A high-resolution digital",
  },
];

db.products.insertMany(productsData);

db.products.aggregate([
  {
    $match: {
      inStock: true,
    },
  },
  {
    $group: {
      _id: "$category",
      numProducts: {
        $sum: 1,
      },
    },
  },
]);

// console.log(
//   `${database}.${collection} has ${db.products.countDocuments()} documents.`
// );

// const inStock = db.products.find({ inStock: true }).toArray();

// fs.writeFileSync("./products.txt", JSON.stringify(inStock));

// db.products.find({ inStock: true });

// use("inventory");

// db.products.createIndex({ inStock: 1 });

// db.products.explain().find({ inStock: true });
