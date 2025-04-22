import json
import random
from datetime import datetime, timedelta
from faker import Faker
import argparse

class HotelDataGenerator:
    def __init__(self):
        self.fake = Faker()
        # Initialize base data
        self.room_types = [
            {"name": "Standard", "price_range": (80, 150)},
            {"name": "Deluxe", "price_range": (120, 200)},
            {"name": "Suite", "price_range": (200, 400)},
            {"name": "Executive", "price_range": (250, 450)},
            {"name": "Penthouse", "price_range": (400, 800)}
        ]
        
        self.hotel_types = ["Luxury", "Business", "Resort", "Boutique", "Historic", 
                          "Hostel", "Lodge", "Eco Lodge", "Beach Resort", "Spa Resort"]
        
        self.amenities_pool = ["Pool", "Spa", "Gym", "Restaurant", "Bar", "Business Center",
                              "Free Parking", "Room Service", "Conference Rooms", "Free WiFi",
                              "Airport Shuttle", "Concierge", "Fitness Center", "Laundry Service"]

    def generate_hotels(self, count):
        hotels = []
        for i in range(count):
            hotel_id = f"hotel_{str(i+1).zfill(3)}"
            
            # Select 2-3 room types for this hotel
            hotel_room_types = random.sample(self.room_types, random.randint(2, 3))
            room_types = []
            for rt in hotel_room_types:
                base_price = random.uniform(rt["price_range"][0], rt["price_range"][1])
                
                # Define features based on room type
                if rt["name"] == "Standard":
                    features = ["TV", "Air Conditioning", "Private Bathroom"]
                elif rt["name"] == "Deluxe":
                    features = ["TV", "Air Conditioning", "Private Bathroom", "Mini Bar", "City View"]
                elif rt["name"] == "Suite":
                    features = ["TV", "Air Conditioning", "Private Bathroom", "Mini Bar", "Living Room", "Kitchenette"]
                elif rt["name"] == "Executive":
                    features = ["TV", "Air Conditioning", "Private Bathroom", "Mini Bar", "Office Space", "Living Room", "Premium View"]
                else: # Penthouse
                    features = ["TV", "Air Conditioning", "Private Bathroom", "Mini Bar", "Living Room", "Kitchen", "Private Terrace", "Panoramic View"]
                
                room_types.append({
                    "name": rt["name"],
                    "price": round(base_price, 2),
                    "availability": random.randint(5, 50),
                    "features": features
                })

            hotel = {
                "hotel_id": hotel_id,
                "name": f"{self.fake.company()} Hotel",
                "address": self.fake.street_address(),
                "rooms": random.randint(50, 300),
                "room_types": room_types,
                "rating": round(random.uniform(3.5, 5.0), 1),
                "reviews": random.randint(50, 1000),
                "amenities": random.sample(self.amenities_pool, random.randint(4, 8)),
                "country": self.fake.country(),
                "city": self.fake.city(),
                "type": random.choice(self.hotel_types),
                "url": f"www.{self.fake.domain_word()}.com",
                "email": f"info@{self.fake.domain_word()}.com",
                "phone": self.fake.phone_number(),
                "has_rewards_program": random.choice([True, False]),
                "geo": {
                    "lat": float(self.fake.latitude()),
                    "long": float(self.fake.longitude())
                }
            }
            hotels.append(hotel)
        return hotels

    def generate_users(self, count):
        users = []
        for i in range(count):
            user_id = f"user_{str(i+1).zfill(3)}"
            name = self.fake.name()
            username = self.fake.user_name()
            
            user = {
                "user_id": user_id,
                "username": username,
                "name": name,
                "email": self.fake.email(),
                "phone": None if random.random() < 0.2 else self.fake.phone_number(),
                "address": self.fake.street_address(),
                "country": self.fake.country(),
                "city": self.fake.city(),
                "zipcode": self.fake.postcode()
            }
            users.append(user)
        return users

    def generate_bookings(self, count, hotels, users):
        bookings = []
        payment_methods = ["Credit Card", "PayPal", "Bank Transfer", "Cash"]
        
        for i in range(count):
            booking_id = f"booking_{str(i+1).zfill(3)}"
            hotel = random.choice(hotels)
            room_type = random.choice(hotel["room_types"])
            checkin_date = self.fake.date_time_between(start_date="-1y", end_date="+1y")
            checkout_date = checkin_date + timedelta(days=random.randint(1, 10))
            
            booking = {
                "booking_id": booking_id,
                "hotel_id": hotel["hotel_id"],
                "user_id": random.choice(users)["user_id"],
                "checkin": checkin_date.isoformat() + "Z",
                "checkout": checkout_date.isoformat() + "Z",
                "room_type": room_type["name"],
                "room_count": random.randint(1, 3),
                "total_price": round(room_type["price"] * random.randint(1, 3), 2),
                "payment_method": random.choice(payment_methods),
                "payment_status": random.choice(["Paid", "Pending", "Cancelled"])
            }
            bookings.append(booking)
        return bookings

    def generate_reviews(self, count, hotels, users):
        reviews = []
        rating_categories = ["Value", "Cleanliness", "Overall", "Check in / front desk", "Rooms"]
        
        for i in range(count):
            review_id = f"review_{str(i+1).zfill(3)}"
            
            ratings = {}
            # Randomly select between 2-5 categories to rate
            categories_to_rate = random.sample(rating_categories, random.randint(2, len(rating_categories)))
            for category in categories_to_rate:
                ratings[category] = random.randint(3, 5)
            
            review = {
                "review_id": review_id,
                "hotel_id": random.choice(hotels)["hotel_id"],
                "review_date": self.fake.date_time_between(start_date="-1y", end_date="now").isoformat() + "Z",
                "user_id": random.choice(users)["user_id"],
                "ratings": ratings,
                "comments": self.fake.paragraph()
            }
            reviews.append(review)
        return reviews

def main():
    parser = argparse.ArgumentParser(description='Generate hotel-related datasets')
    parser.add_argument('--hotels', type=int, default=20, help='Number of hotels to generate')
    parser.add_argument('--users', type=int, default=80, help='Number of users to generate')
    parser.add_argument('--bookings', type=int, default=100, help='Number of bookings to generate')
    parser.add_argument('--reviews', type=int, default=40, help='Number of reviews to generate')
    
    args = parser.parse_args()
    
    generator = HotelDataGenerator()
    
    # Generate data
    hotels = generator.generate_hotels(args.hotels)
    users = generator.generate_users(args.users)
    bookings = generator.generate_bookings(args.bookings, hotels, users)
    reviews = generator.generate_reviews(args.reviews, hotels, users)
    
    # Save to files
    with open('hotel.json', 'w') as f:
        json.dump(hotels, f, indent=2)
    
    with open('users.json', 'w') as f:
        json.dump(users, f, indent=2)
    
    with open('bookings.json', 'w') as f:
        json.dump(bookings, f, indent=2)
    
    with open('reviews.json', 'w') as f:
        json.dump(reviews, f, indent=2)
    
    print(f"Generated {len(hotels)} hotels")
    print(f"Generated {len(users)} users")
    print(f"Generated {len(bookings)} bookings")
    print(f"Generated {len(reviews)} reviews")

if __name__ == "__main__":
    main()
