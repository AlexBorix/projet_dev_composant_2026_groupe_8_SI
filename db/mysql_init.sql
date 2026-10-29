-- Base source : ventes
CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE IF NOT EXISTS sales (
  id INT PRIMARY KEY AUTO_INCREMENT,
  customer VARCHAR(100) NOT NULL,
  product  VARCHAR(100) NOT NULL,
  amount   DECIMAL(10,2) NOT NULL,
  sold_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO sales (customer, product, amount) VALUES
 ('Awa',    'Clavier', 45.90),
 ('Issa',   'Souris',  19.99),
 ('Fatou',  'Ecran',  199.00),
 ('Moussa', 'Casque',  59.50),
 ('Aicha',  'Webcam',  39.00);

-- Utilisateur dedie a Airbyte (lecture seule)
CREATE USER IF NOT EXISTS 'airbyte'@'%' IDENTIFIED BY 'airbytepass';
GRANT SELECT ON shop.* TO 'airbyte'@'%';
FLUSH PRIVILEGES;
