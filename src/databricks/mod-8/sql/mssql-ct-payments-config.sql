-- DROP TABLE dbo.Payments;
-- SELECT * FROM dbo.Payments;

CREATE TABLE dbo.Payments
(
  payment_id           UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
  order_key            UNIQUEIDENTIFIER NOT NULL,
  invoice_id           NVARCHAR(64)     NOT NULL,
  provider             NVARCHAR(64)     NOT NULL,
  method               NVARCHAR(32)     NOT NULL,
  status               NVARCHAR(32)     NOT NULL,
  currency             NCHAR(3)         NOT NULL,
  amount               DECIMAL(18,2)    NOT NULL,
  tax_amount           DECIMAL(18,2)    NULL,
  net_amount           DECIMAL(18,4)    NULL,
  provider_fee         DECIMAL(18,4)    NULL,
  platform_fee         DECIMAL(18,4)    NULL,
  captured             BIT               NOT NULL,
  refunded             BIT               NOT NULL,
  capture_timestamp    DATETIME2(6)      NULL,
  [timestamp]          DATETIME2(6)      NOT NULL,
  card_brand           NVARCHAR(32)     NULL,
  card_last4           CHAR(4)          NULL,
  card_exp_month       TINYINT          NULL,
  card_exp_year        SMALLINT         NULL,
  wallet_provider      NVARCHAR(32)     NULL,
  failure_reason       NVARCHAR(256)    NULL,
  receipt_url          NVARCHAR(512)    NULL,
  country              NCHAR(2)         NULL,
  ip_address           NVARCHAR(64)     NULL,
  user_agent           NVARCHAR(512)    NULL,
  dt_current_timestamp DATETIME2(6)     NULL
);
GO

ALTER DATABASE owshq
SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);
GO

ALTER TABLE dbo.Payments ENABLE CHANGE_TRACKING;


USE owshq
INSERT INTO dbo.Payments
(
  payment_id, order_key, invoice_id, provider, method, status, currency,
  amount, tax_amount, net_amount, provider_fee, platform_fee,
  captured, refunded, capture_timestamp, [timestamp],
  card_brand, card_last4, card_exp_month, card_exp_year, wallet_provider,
  failure_reason, receipt_url, country, ip_address, user_agent, dt_current_timestamp
)
VALUES
(
  '0a8287b5-52d2-ece5-a0f2-697d8da268b3',
  '77a7f712-95f6-413f-e9b8-0065a8d3aac6',
  'INV-54965', 'Adyen', 'Boleto', 'succeeded', 'BRL',
  87.63, 9.44, 76.37, 0.73, 1.09,
  1, 1, '2025-10-05 18:06:40.183', '2025-10-05 18:06:40.183',
  'Visa', '6800', 9, 2029, 'None',
  '', 'https://www.bryan-sipes.io:9422/', 'US', '62.44.48.135',
  'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
  '2025-10-05 18:06:40.183'
);

SELECT * FROM Payments

INSERT INTO dbo.Payments VALUES
(NEWID(), NEWID(), 'INV-54966', 'Stripe', 'Card', 'succeeded', 'BRL', 120.00, 10.00, 108.50, 0.90, 0.60,
  1, 0, SYSDATETIME(), SYSDATETIME(), 'Mastercard', '1234', 12, 2030, 'ApplePay', NULL, NULL, 'BR', '10.0.0.1', 'UA/1', SYSDATETIME()),
(NEWID(), NEWID(), 'INV-54967', 'Adyen', 'Pix', 'processing', 'BRL', 50.00, 5.00, 44.50, 0.40, 0.10,
  0, 0, NULL, SYSDATETIME(), NULL, NULL, NULL, NULL, 'None', NULL, NULL, 'BR', '10.0.0.2', 'UA/2', SYSDATETIME());

-- DELETE FROM dbo.Payments WHERE invoice_id = 'INV-54966'
-- DELETE FROM dbo.Payments WHERE invoice_id = 'INV-54967'

UPDATE dbo.Payments
SET status = 'refunded', refunded = 1, net_amount = net_amount - 20.00
WHERE invoice_id = 'INV-54966';

DELETE FROM dbo.Payments
WHERE invoice_id = 'INV-54967';

INSERT INTO dbo.Payments (payment_id, order_key, invoice_id, provider, method, status, currency,
  amount, tax_amount, net_amount, provider_fee, platform_fee,
  captured, refunded, capture_timestamp, [timestamp],
  card_brand, card_last4, card_exp_month, card_exp_year, wallet_provider,
  failure_reason, receipt_url, country, ip_address, user_agent, dt_current_timestamp)
VALUES
(NEWID(), NEWID(), 'INV-70001', 'Adyen', 'Card', 'succeeded', 'BRL',
  199.99, 19.99, 179.20, 1.10, 1.70, 1, 0, SYSDATETIME(), SYSDATETIME(),
  'Visa', '4321', 8, 2031, 'None', NULL, NULL, 'BR', '10.0.0.3', 'UA/3', SYSDATETIME());

SELECT * FROM Payments