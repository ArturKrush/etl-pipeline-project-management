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
('  Enterprise Cloud Migration  ', 'Migrating core services to AWS', '2024-03-01', NULL, 'Marcus Johnson', 'Development', 'Active', '150000.00', '300000'),
('Data Warehouse Optimization', 'Refactoring Snowflake schemas', '2024-01-15', '2024-05-20', 'Steven Wong', 'Data Analysis', 'Completed', '45000', '120000'),
('123 Invalid Name Project', 'Valid description compensates', '2024-06-01', NULL, 'Emily Chen', 'Management', 'Active', '12000', '0'), -- БИТИЙ NAME, але є Description -> ПРОЙДЕ (Name стане NULL)
(NULL, NULL, '2024-05-01', NULL, 'Robert Jenkins', 'SEO', 'Active', '5000', '10000'), -- БИТИЙ: Немає ні Name, ні Desc -> ВИДАЛИТЬСЯ
('AI Customer Support', 'LLM integration', '2024-08-01', NULL, 'Amanda Waller', NULL, 'Active', '85000', '200000'), -- БИТИЙ: Немає Field -> ВИДАЛИТЬСЯ
('Legacy System Decommission', 'Shutting down old servers', '2021-02-10', '2021-12-31', NULL, 'Development', NULL, '0', '50000'), -- БИТИЙ: Немає Manager, Status і дата в минулому -> ВИДАЛИТЬСЯ
('Cybersecurity Audit Q3', 'External penetration testing', '2024-07-01', '2024-09-30', 'Marcus Johnson', 'Development', 'Active', '30000', '0'),
('  Enterprise Cloud Migration  ', 'Migrating core services to AWS', '2024-03-01', NULL, 'Marcus Johnson', 'Development', 'Active', '150000.00', '300000'), -- ПОВНИЙ ДУБЛІКАТ -> ВИДАЛИТЬСЯ
('Q4 Marketing Campaign', 'Social media and SEO push', '2024-10-01', '2024-12-31', 'Lucas Pierce', 'Content Strategy', 'Active', '25000', '75000'),
('Internal Portal Redesign', 'Intranet UI/UX updates', '2024-02-01', '2024-06-01', 'Emily Chen', 'Development', 'Completed', '18000', '10000'),
('Compliance Review 2024', 'GDPR and CCPA audit', '2024-01-10', '2024-03-15', 'Robert Jenkins', 'Management', 'Completed', '15000', '0'),
('!@# Bad Project Name', NULL, '2024-09-01', NULL, 'Amanda Waller', 'Data Analysis', 'Suspended', '10000', '0'), -- БИТИЙ: Невалідне ім'я ТА немає Description -> ВИДАЛИТЬСЯ
('Future Tech Exploration', 'Researching quantum computing applications', '2026-01-01', NULL, NULL, 'Development', NULL, '50000', '0'), -- ПРОЙДЕ: Дата в майбутньому рятує від видалення через відсутність Manager/Status
('Candidate Tracking System', 'Implementing new ATS', '2024-04-15', NULL, 'Sarah OConnor', 'Talent Acquisition', 'Active', '20000', '60000'),
('BI Dashboard Rollout', 'Tableau dashboards for C-level', '2024-05-01', NULL, 'Steven Wong', 'Data Analysis', 'Active', '35000', '100000');