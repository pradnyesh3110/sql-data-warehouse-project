1. gold.dim_customer

- Purpose: Stores customer details enriched with demographic and geographic data.
- Columns:
- Column Name     | Data Type    | Description

  customer_key    | INT          | A surrogate key that uniquely identifies each customer in this table.
  customer_id     |  INT         | A unique number assigned to each customer for identification.
  customer_number | NVARCHAR(50) | An alphanumeric code used for tracking and referencing the customer.
  last_name       | NVARCHAR(50) | The customer's last name (or family name).
  country         | NVARCHAR(50) | The country where the customer resides (e.g., 'Australia').
  marital_status  | NVARCHAR(50) | Indicates whether the customer is 'Married', 'Single', etc.
  gender          | NVARCHAR(50) | Specifies the customer's gender (e.g., 'Male', 'Female', 'n/a').
  birthdate       | DATE         | The date of birth of the customer, formatted as YYYY-MM-DD (e.g., 1971-10-06).
  create_date     | DATE         | The date and time when the customer record was created in the system.

  2.gold.dim_products
-Purpose: Provides information about the products and their attributes.
- Columns
- Column Name         | Data Type    | Description

 product_key          | INT          | Surrogate key uniquely identify each product record in the product dimension table.
 product_id           | INT          | A unique identifier assigned to the product for internal tracking and referencing.
 product_number       | NVARCHAR(50) | A structured alphanumeric code representing the product, often used for categorizing inventory.
 product_name         | NVARCHAR(50) | Descriptive name of the product, including key details such as type, color, and size.
 category_id          | NVARCHAR(50) | A unique identifier for the product's category, linking to its high-level classification.
 category             | NVARCHAR(50) | The broader classification of the product (e.g., Bikes, Components) to group related items.
 subcategory          | NVARCHAR(50) | A more detailed classification of the product within the category, such as product type.
 maintenance_required | NVARCHAR(50) | Indicates whether the product requires maintenance (e.g., 'Yes', 'No').
 cost                 | INT          | The cost or base price of the product, measured in monetary units.
 product_line         | NVARCHAR(50) | The specific product line or series to which the product belongs (e.g., Road, Mountain).
 start_date           | DATE         | The date when the product became available for sale or use, stored in the system.


 


