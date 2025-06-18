CREATE TABLE "olist_orders_dataset" (
  "order_id" varchar PRIMARY KEY,
  "customer_id" varchar NOT NULL,
  "order_status" varchar,
  "order_purchase_timestamp" timestamp,
  "order_approved_at" timestamp,
  "order_delivered_carrier_date" timestamp,
  "order_delivered_customer_date" timestamp,
  "order_estimated_delivery_date" timestamp
);

COPY olist_orders_dataset FROM 'olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_order_reviews_dataset" (
  "review_id" varchar PRIMARY KEY,
  "order_id" varchar NOT NULL,
  "review_score" int,
  "review_comment_title" varchar,
  "review_comment_message" varchar,
  "review_creation_date" timestamp,
  "review_answer_timestamp" timestamp
);

COPY olist_order_reviews_dataset FROM 'olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_order_payments_dataset" (
  "order_id" varchar NOT NULL,
  "payment_sequential" int NOT NULL,
  "payment_type" varchar,
  "payment_installments" int,
  "payment_value" decimal,
  PRIMARY KEY ("order_id", "payment_sequential")
);

COPY olist_order_payments_dataset FROM 'olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_customers_dataset" (
  "customer_id" varchar PRIMARY KEY,
  "customer_unique_id" varchar,
  "customer_zip_code_prefix" int,
  "customer_city" varchar,
  "customer_state" varchar
);

COPY olist_customers_dataset FROM 'olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_order_items_dataset" (
  "order_id" varchar NOT NULL,
  "order_item_id" int NOT NULL,
  "product_id" varchar NOT NULL,
  "seller_id" varchar NOT NULL,
  "shipping_limit_date" timestamp,
  "price" decimal,
  "freight_value" decimal,
  PRIMARY KEY ("order_id", "order_item_id")
);

COPY olist_order_items_dataset FROM 'olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_products_dataset" (
  "product_id" varchar PRIMARY KEY,
  "product_category_name" varchar,
  "product_name_lenght" decimal,
  "product_description_lenght" decimal,
  "product_photos_qty" decimal,
  "product_weight_g" decimal,
  "product_length_cm" decimal,
  "product_height_cm" decimal,
  "product_width_cm" decimal
);

COPY olist_products_dataset FROM 'olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_sellers_dataset" (
  "seller_id" varchar PRIMARY KEY,
  "seller_zip_code_prefix" int,
  "seller_city" varchar,
  "seller_state" varchar
);

COPY olist_sellers_dataset FROM 'olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "olist_geolocation_dataset" (
  "geolocation_zip_code_prefix" int,
  "geolocation_lat" decimal,
  "geolocation_lng" decimal,
  "geolocation_city" varchar,
  "geolocation_state" varchar
);

COPY olist_geolocation_dataset FROM 'olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

---

CREATE TABLE "product_category_name_translation" (
  "product_category_name" varchar PRIMARY KEY,
  "product_category_name_english" varchar
);

COPY product_category_name_translation FROM 'product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

---

ALTER TABLE "olist_orders_dataset" 
  ADD FOREIGN KEY ("customer_id") REFERENCES "olist_customers_dataset" ("customer_id");

ALTER TABLE "olist_order_reviews_dataset" 
  ADD FOREIGN KEY ("order_id") REFERENCES "olist_orders_dataset" ("order_id");

ALTER TABLE "olist_order_payments_dataset" 
  ADD FOREIGN KEY ("order_id") REFERENCES "olist_orders_dataset" ("order_id");

ALTER TABLE "olist_order_items_dataset" 
  ADD FOREIGN KEY ("order_id") REFERENCES "olist_orders_dataset" ("order_id");

ALTER TABLE "olist_order_items_dataset" 
  ADD FOREIGN KEY ("product_id") REFERENCES "olist_products_dataset" ("product_id");

ALTER TABLE "olist_order_items_dataset" 
  ADD FOREIGN KEY ("seller_id") REFERENCES "olist_sellers_dataset" ("seller_id");

ALTER TABLE "olist_products_dataset" 
  ADD FOREIGN KEY ("product_category_name") REFERENCES "product_category_name_translation" ("product_category_name");