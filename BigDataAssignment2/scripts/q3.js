// Search for products using a keyword
db.events.find({ category_code: { $regex: "electronics", $options: "i" } }, { product_id: 1, category_code: 1 }).limit(10);
