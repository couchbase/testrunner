SELECT h.name, (SELECT COUNT(*) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS review_count FROM hotel h WHERE h.rating > 4.0;
SELECT u.name, (SELECT AVG(r.ratings.Overall) FROM reviews r WHERE r.user_id = u.user_id) AS avg_rating FROM users u WHERE u.city = 'New York';
SELECT h.name, (SELECT MAX(room.price) FROM h.room_types room) AS max_room_price FROM hotel h WHERE h.free_wifi = true;
SELECT b.user_id, (SELECT MIN(b.checkin) FROM bookings b WHERE b.user_id = u.user_id) AS earliest_checkin FROM users u WHERE u.country = 'USA';
SELECT h.city, (SELECT COUNT(DISTINCT b.hotel_id) FROM bookings b WHERE h.hotel_id = b.hotel_id) AS booking_count FROM hotel h WHERE h.free_parking = true;
SELECT u.name, (SELECT SUM(b.total_price) FROM bookings b WHERE b.user_id = u.user_id) AS total_spent FROM users u WHERE u.zipcode = '10001';
SELECT h.name, (SELECT AVG(r.ratings.Cleanliness) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_cleanliness FROM hotel h WHERE h.country = 'USA';
SELECT h.name, (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id) AS total_bookings FROM hotel h WHERE h.rooms > 50;
SELECT u.name, (SELECT COUNT(*) FROM reviews r WHERE r.user_id = u.user_id AND r.ratings.Overall = 5) AS five_star_reviews FROM users u WHERE u.email IS NOT MISSING;
SELECT h.name, (SELECT MIN(room.availability) FROM h.room_types room WHERE room.price < 200) AS min_availability FROM hotel h WHERE h.city = 'New York';
SELECT b.hotel_id, (SELECT SUM(room.price * b.room_count) FROM hotel h UNNEST h.room_types room WHERE room.name = b.room_type AND b.hotel_id = h.hotel_id) AS total_room_price FROM bookings b WHERE b.payment_status = 'Paid';
SELECT u.name, (SELECT COUNT(*) FROM reviews r WHERE r.user_id = u.user_id AND r.ratings.Overall < 3) AS low_rating_reviews FROM users u WHERE u.phone IS NOT MISSING;
SELECT COUNT(*) AS total_hotels FROM (SELECT h.hotel_id FROM hotel h WHERE h.geo.lat BETWEEN 40.5 AND 41.0) AS hotels;
SELECT h.city, (SELECT COUNT(DISTINCT r.user_id) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS unique_reviewers FROM hotel h WHERE h.city = 'Los Angeles';
SELECT u.user_id, (SELECT MAX(b.checkout) FROM bookings b WHERE b.user_id = u.user_id) AS latest_checkout FROM users u WHERE u.country = 'USA';
SELECT h.name, (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id AND b.room_type = 'Single') AS single_room_bookings FROM hotel h WHERE h.review < 100;
SELECT u.user_id, (SELECT COUNT(DISTINCT r.hotel_id) FROM reviews r WHERE r.user_id = u.user_id) AS distinct_hotels_reviewed FROM users u WHERE u.city = 'New York';
SELECT h.name, (SELECT SUM(room.availability) FROM h.room_types room WHERE room.availability > 0) AS sum_availability FROM hotel h WHERE h.country = 'USA';
SELECT u.name, (SELECT COUNT(*) FROM bookings b WHERE b.user_id = u.user_id AND b.payment_status = 'Unpaid') AS unpaid_bookings FROM users u WHERE u.email IS NOT MISSING;
SELECT h.name, (SELECT MAX(room.price) FROM h.room_types room WHERE room.availability > 0) AS highest_price_available FROM hotel h WHERE h.city = 'San Francisco';
SELECT h.hotel_id, (SELECT MIN(b.checkin) FROM bookings b WHERE b.hotel_id = h.hotel_id) AS first_booking FROM hotel h WHERE h.free_wifi = true;
SELECT u.user_id, (SELECT AVG(r.ratings.Rooms) FROM reviews r WHERE r.user_id = u.user_id) AS avg_room_rating FROM users u WHERE u.country = 'Canada';
SELECT h.name, (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id AND b.payment_method = 'Credit Card') AS credit_card_bookings FROM hotel h;
SELECT u.username, (SELECT SUM(b.room_count) FROM bookings b WHERE b.user_id = u.user_id) AS total_rooms_booked FROM users u WHERE u.zipcode = '90210';
SELECT u.name, (SELECT COUNT(*) FROM bookings b WHERE b.user_id = u.user_id AND b.room_type = 'Double') AS double_room_bookings FROM users u;
SELECT h.type, (SELECT AVG(h.rating) FROM hotel WHERE h.type = hotel_type) AS avg_rating_per_type FROM hotel h;
SELECT b.hotel_id, (SELECT MAX(room.price) FROM hotel h UNNEST h.room_types room WHERE b.hotel_id = h.hotel_id AND room.price IS NOT MISSING) AS max_price FROM bookings b;
SELECT u.user_id, (SELECT MIN(b.total_price) FROM bookings b WHERE b.user_id = u.user_id) AS min_total_price FROM users u;
SELECT h.name, (SELECT COUNT(*) FROM reviews r WHERE r.hotel_id = h.hotel_id AND r.comments IS NOT MISSING) AS count_reviews_with_comments FROM hotel h;
SELECT COUNT(DISTINCT u.user_id) AS distinct_users_with_reviews FROM reviews r JOIN users u ON r.user_id = u.user_id WHERE u.country = 'USA';
SELECT h.hotel_id, (SELECT MIN(r.ratings.Overall) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS lowest_overall_rating FROM hotel h;
SELECT COUNT(*) AS total_reviewed_hotels FROM (SELECT h.hotel_id FROM hotel h WHERE h.reviews > 100) AS reviewed_hotels;
SELECT h.name, (SELECT AVG(r.ratings.Rooms) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_room_rating FROM hotel h WHERE h.type = 'Resort';
SELECT u.name, (SELECT COUNT(*) FROM reviews r WHERE r.user_id = u.user_id) AS review_count FROM users u WHERE u.email IS NOT MISSING;
SELECT h.name, (SELECT MAX(r.ratings.Cleanliness) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS max_cleanliness FROM hotel h WHERE h.city = 'Chicago';
SELECT b.booking_id, (SELECT SUM(room.availability) FROM hotel h UNNEST h.room_types room WHERE room.price < 150 AND b.hotel_id = h.hotel_id) AS total_availability FROM bookings b;
SELECT COUNT(*) AS total_free_parking_hotels FROM (SELECT h.hotel_id FROM hotel h WHERE h.free_parking = true) AS free_parking_hotels;
SELECT h.type, (SELECT MIN(h.rating) FROM hotel WHERE type = h.type) AS min_rating_per_type FROM hotel h;
SELECT h.name, (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id AND b.payment_method = 'Paypal') AS paypal_bookings FROM hotel h;
SELECT h.hotel_id, (SELECT MAX(b.total_price) FROM bookings b WHERE b.hotel_id = h.hotel_id) AS max_total_price FROM hotel h;
SELECT u.name, (SELECT COUNT(*) FROM bookings b WHERE b.user_id = u.user_id) AS total_bookings FROM users u WHERE u.city = 'Miami';
SELECT h.name, (SELECT SUM(room.price) FROM h.room_types room WHERE room.price > 200) AS sum_premium_room_prices FROM hotel h;
SELECT u.email, (SELECT COUNT(*) FROM reviews r WHERE r.user_id = u.user_id) AS total_reviews FROM users u WHERE u.zipcode = '30301';
SELECT h.hotel_id, (SELECT AVG(r.ratings.Cleanliness) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_cleanliness_rating FROM hotel h;
SELECT u.user_id, (SELECT MAX(b.room_count) FROM bookings b WHERE b.user_id = u.user_id) AS max_rooms_booked FROM users u;
SELECT h.name, (SELECT AVG(room.availability) FROM h.room_types room) AS avg_room_availability FROM hotel h WHERE h.reviews > 50;
SELECT COUNT(*) AS total_hotels_with_wifi FROM (SELECT h.hotel_id FROM hotel h WHERE h.free_wifi = true) AS hotels_with_wifi;
SELECT u.name, (SELECT COUNT(*) FROM bookings b WHERE b.user_id = u.user_id AND b.payment_status = 'Pending') AS pending_bookings FROM users u WHERE u.country = 'Canada';
SELECT h.name, (SELECT MIN(r.ratings.Overall) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS min_overall_rating FROM hotel h WHERE h.country = 'UK';
SELECT b.hotel_id, (SELECT AVG(r.ratings.Rooms) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_rooms_rating FROM bookings b JOIN hotel h ON b.hotel_id = h.hotel_id WHERE b.payment_status = 'Paid';
SELECT h.name, (SELECT SUM(room.price) FROM h.room_types room) AS total_room_types_price FROM hotel h WHERE h.city = 'Orlando';
SELECT COUNT(*) AS total_users_with_bookings FROM (SELECT u.user_id FROM users u JOIN bookings b ON u.user_id = b.user_id) AS users_with_bookings;
SELECT h.name, (SELECT MIN(b.total_price) FROM bookings b WHERE b.hotel_id = h.hotel_id) AS min_booking_price FROM hotel h WHERE h.free_parking = true;
SELECT u.user_id, (SELECT COUNT(*) FROM reviews r WHERE r.user_id = u.user_id AND r.ratings.Overall = 1) AS one_star_reviews FROM users u;
SELECT h.type, (SELECT AVG(h.rating) FROM hotel WHERE type = hotel_type) AS avg_rating_by_type FROM hotel h;
SELECT h.name, (SELECT COUNT(*) FROM reviews r WHERE r.hotel_id = h.hotel_id AND r.ratings.Overall >= 4) AS positive_reviews FROM hotel h WHERE h.city = 'Paris';
SELECT u.user_id, (SELECT MIN(b.total_price) FROM bookings b WHERE b.user_id = u.user_id) AS min_spending FROM users u;
SELECT h.name, (SELECT SUM(room.availability) FROM h.room_types room) AS total_availability FROM hotel h WHERE h.free_wifi = true;
SELECT COUNT(DISTINCT u.user_id) AS unique_reviewers FROM reviews r JOIN users u ON r.user_id = u.user_id WHERE u.country = 'Australia';
SELECT h.hotel_id, (SELECT AVG(r.ratings.Cleanliness) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_cleanliness FROM hotel h WHERE h.rating > 3.0;
SELECT u.name, (SELECT COUNT(DISTINCT r.hotel_id) FROM reviews r WHERE r.user_id = u.user_id AND r.ratings.Overall = 5) AS five_star_hotels FROM users u;
SELECT COUNT(*) AS total_hotels_in_city FROM (SELECT h.hotel_id FROM hotel h WHERE h.city = 'New York') AS ny_hotels;
SELECT h.name, (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id AND b.payment_method = 'Debit Card') AS debit_card_bookings FROM hotel h;
SELECT h.name, (SELECT MIN(r.ratings.Overall) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS min_overall_rating FROM hotel h WHERE h.reviews > 50;
SELECT b.user_id, (SELECT COUNT(*) FROM bookings b WHERE b.user_id = u.user_id AND b.payment_method = 'Bitcoin') AS bitcoin_bookings FROM users u;
SELECT h.name, (SELECT AVG(r.ratings.Rooms) FROM reviews r WHERE r.hotel_id = h.hotel_id) AS avg_rooms_rating FROM hotel h WHERE h.reviews < 200;
SELECT COUNT(*) AS total_city_hotels FROM (SELECT h.hotel_id FROM hotel h WHERE h.city = 'Amsterdam') AS amsterdam_hotels;
SELECT u.name, (SELECT MAX(b.total_price) FROM bookings b WHERE b.user_id = u.user_id) AS max_spending FROM users u WHERE u.country = 'Germany';
