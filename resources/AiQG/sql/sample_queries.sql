SELECT h.hotel_id, h.name AS hotel_name, h.city, h.country, h.rating, booking_stats.total_bookings, booking_stats.avg_price, booking_stats.most_booked_room FROM hotel h JOIN (SELECT b.hotel_id, COUNT(*) AS total_bookings, AVG(b.total_price) AS avg_price, ARRAY_AGG(b.room_type)[0] AS most_booked_room FROM bookings b GROUP BY b.hotel_id HAVING COUNT(*) > 2) AS booking_stats ON h.hotel_id = booking_stats.hotel_id WHERE h.rating > 4.0 ORDER BY booking_stats.total_bookings DESC, h.rating DESC;
 SELECT h.name AS name1, rt.name as name2, rt.price FROM hotel AS h UNNEST h.room_types AS rt UNNEST h.room_types AS rt2 WHERE rt.availability > 0 AND rt.price < rt2.price;
 SELECT h.name, rt.name FROM hotel AS h UNNEST h.room_types AS rt UNNEST h.room_types AS rt2 WHERE rt.name = rt2.name AND rt.availability <> rt2.availability;
 SELECT h.name, rt.name, r.name FROM hotel AS h UNNEST h.room_types AS rt UNNEST reviews AS r WHERE h.hotel_id = r.hotel_id AND rt.availability > 10 AND r.ratings.Overall > 4;
 SELECT h.name, rt.name, r.comments FROM hotel AS h UNNEST h.room_types AS rt UNNEST reviews AS r WHERE h.hotel_id = r.hotel_id AND r.ratings.Check_in /front desk > 3;
 SELECT h.name, rt.price, b.total_price FROM hotel AS h UNNEST h.room_types AS rt UNNEST bookings AS b WHERE h.hotel_id = b.hotel_id AND rt.name = b.room_type;
 SELECT u.name, h.name, b.total_price FROM users AS u UNNEST bookings AS b UNNEST hotel AS h WHERE u.user_id = b.user_id AND h.hotel_id = b.hotel_id AND b.payment_status = 'Completed';
 SELECT h.name, rt1.name AS room_type1, rt2.name AS room_type2 FROM hotel AS h UNNEST h.room_types AS rt1 UNNEST h.room_types AS rt2 WHERE rt1.price > rt2.price;
 SELECT h.name, rt.name, rt2.name FROM hotel AS h UNNEST h.room_types AS rt UNNEST h.room_types AS rt2 WHERE rt.name != rt2.name AND rt.availability + rt2.availability > 20;
 SELECT h.name, rt.name, r.ratings.Overall FROM hotel AS h UNNEST h.room_types AS rt UNNEST reviews AS r WHERE h.hotel_id = r.hotel_id AND rt.availability BETWEEN 5 AND 15;

