CREATE TABLE source_table (
    id SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    Description TEXT,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    Manager VARCHAR(100),
    Field VARCHAR(100),
    Status VARCHAR(50),
    Cost VARCHAR(50),
    Benefit VARCHAR(50)
);

INSERT INTO source_table (Name, Description, StartDate, EndDate, Manager, Field, Status, Cost, Benefit) VALUES
('Cloud Infrastructure Migration', 'Moving core services to AWS', '2024-01-15', NULL, 'Marcus Johnson', 'Development', 'Active', '150000', '300000'),
('Customer Segmentation Model', 'K-means clustering for marketing', '2024-02-01', '2024-05-01', 'Steven Wong', 'Data Analysis', 'Completed', '45000', '120000'),
('1st Quarter Brand Audit', NULL, '2024-03-10', NULL, 'Emily Chen', 'Content Strategy', 'Active', '12000', '0'), -- БИТИЙ: Назва з цифри ТА немає опису -> ВИДАЛИТЬСЯ
('Data Lake Architecture', 'Implementing Snowflake', '2024-04-01', NULL, 'Robert Jenkins', NULL, 'Active', '85000', '200000'), -- БИТИЙ: Немає Field -> ВИДАЛИТЬСЯ
('Legacy API Deprecation', 'Shutting down v1 endpoints', '2021-06-15', '2021-12-31', NULL, 'Development', NULL, '0', '50000'), -- БИТИЙ: Старий, немає менеджера/статусу -> ВИДАЛИТЬСЯ
('Cloud Infrastructure Migration', 'Moving core services to AWS', '2024-01-15', NULL, 'Marcus Johnson', 'Development', 'Active', '150000', '300000'), -- ПОВНИЙ ДУБЛІКАТ рядка 1 -> ВИДАЛИТЬСЯ
('Cybersecurity Penetration Test', 'External audit Q3', '2024-07-01', '2024-09-30', 'Amanda Waller', 'Development', 'Completed', '30000', '0'),
('!@# Experimental UI', 'Redesigning user dashboard', '2024-08-01', NULL, 'Emily Chen', 'Development', 'Suspended', '15000', '0'), -- ПРОЙДЕ: Невалідне ім'я, але є опис. Отримає тег [INVALID NAME]
('SEO Keyword Expansion', 'Targeting long-tail keywords', '2024-09-15', NULL, 'Lucas Pierce', 'SEO', 'Active', '8000', '25000'),
('Executive Onboarding Program', 'C-level training material', '2024-01-10', '2024-03-01', 'Amanda Waller', 'Talent Acquisition', 'Completed', '25000', '0'),
('Real-time Analytics Dashboard', 'PowerBI integration', '2024-10-01', NULL, 'Steven Wong', 'Data Analysis', 'Active', '35000', '100000'),
('Intranet Portal Upgrade', 'SharePoint migration', '2024-05-01', '2024-11-01', 'Robert Jenkins', 'Management', 'Active', '40000', '15000'),
('Social Media Q4 Strategy', 'Campaign planning', '2024-10-15', '2024-12-31', 'Emily Chen', 'Content Strategy', 'Active', '15000', '50000'),
('Backend Microservices Refactor', 'Splitting the monolith', '2024-02-15', NULL, 'Marcus Johnson', 'Development', 'Active', '200000', '400000'),
('University Recruitment Drive', 'Campus hiring events', '2024-03-01', '2024-06-01', 'Sarah OConnor', 'Talent Acquisition', 'Completed', '18000', '0');