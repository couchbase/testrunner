SELECT h.name, rt1.name AS room_type1, rt2.name AS room_type2 FROM hotel h UNNEST h.room_types AS rt1 UNNEST h.room_types AS rt2 WHERE rt1.name != rt2.name ORDER BY h.name;
SELECT h.name, rt.name AS room_type, rt.price FROM hotel h UNNEST h.room_types AS rt LET total_rooms = (SELECT COUNT(*) FROM bookings b WHERE b.hotel_id = h.hotel_id AND b.room_type = rt.name), available_rooms = rt.availability - total_rooms[0] WHERE available_rooms > 0 ORDER BY rt.price;
SELECT h.name, h.address, rt1.name AS room_type1, rt2.name AS room_type2 FROM hotel h UNNEST h.room_types AS rt1 UNNEST h.room_types AS rt2 LET avg_price = (rt1.price + rt2.price) / 2 WHERE rt1.name != rt2.name AND avg_price < 200 ORDER BY avg_price DESC;
SELECT h.name, rw.comments, rw.ratings.Overall AS overall_rating FROM hotel h JOIN reviews rw ON h.hotel_id = rw.hotel_id WHERE rw.ratings.Overall > 4 ORDER BY rw.ratings.Overall DESC;
SELECT b.booking_id, h.name, rt.price * b.room_count AS total_stay_price FROM bookings b JOIN hotel h ON b.hotel_id = h.hotel_id UNNEST h.room_types AS rt WHERE rt.name = b.room_type AND b.checkin >= "2023-10-01" AND b.checkout <= "2023-10-31" ORDER BY total_stay_price ASC;
SELECT h.city, AVG(rw.ratings.Overall) AS average_rating FROM hotel h JOIN reviews rw ON h.hotel_id = rw.hotel_id GROUP BY h.city HAVING AVG(rw.ratings.Overall) > 4 ORDER BY average_rating DESC;
SELECT b.booking_id, u.name, b.total_price FROM bookings b JOIN users u ON b.user_id = u.user_id WHERE b.payment_status = "Paid" ORDER BY b.total_price DESC;
SELECT h.name, h.rating FROM hotel h ORDER BY h.rating DESC;
SELECT b.booking_id, u.username FROM bookings b JOIN users u ON b.user_id = u.user_id ORDER BY b.checkin DESC;
SELECT h.city, COUNT(*) AS hotels_count FROM hotel h GROUP BY h.city ORDER BY hotels_count DESC;
SELECT u.username, SUM(b.total_price) AS total_spent FROM bookings b JOIN users u ON b.user_id = u.user_id WHERE b.payment_status = "Paid" GROUP BY u.username ORDER BY total_spent DESC;
SELECT rw.review_id, rw.comments FROM reviews rw ORDER BY rw.review_date DESC;
