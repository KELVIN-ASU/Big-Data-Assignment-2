// Check which campaigns led to purchases
db.messages.aggregate([
    { $match: { is_purchased: true } },
    { $group: { _id: "$campaign_id", purchase_count: { $sum: 1 } } },
    { $sort: { purchase_count: -1 } },
    { $limit: 10 }
]);
