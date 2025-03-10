// Find the most purchased products
db.events.aggregate([
    { $match: { event_type: "purchase" } },
    { $group: { _id: "$product_id", purchase_count: { $sum: 1 } } },
    { $sort: { purchase_count: -1 } },
    { $limit: 10 }
]);
