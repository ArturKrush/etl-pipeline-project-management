CREATE TABLE Managers (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE Statuses (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE Fields (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE final_table (
    ProjectKey VARCHAR(36) PRIMARY KEY,
    Name VARCHAR(80),
    Description VARCHAR(512),
    StartDate DATETIME NOT NULL,
    EndDate DATETIME,
    ManagerId INT,
    FieldId INT NOT NULL,
    StatusId INT NOT NULL,
    Cost DECIMAL(18,2),
    Benefit DECIMAL(18,2),
    FOREIGN KEY (ManagerId) REFERENCES Managers(Id),
    FOREIGN KEY (StatusId) REFERENCES Statuses(Id),
    FOREIGN KEY (FieldId) REFERENCES Fields(Id)
);

-- Наповнення Look-up таблиць Field та Status
INSERT INTO Statuses (Name) VALUES ('Active'), ('Suspended'), ('Completed'), ('Unknown');
INSERT INTO Fields (Name) VALUES ('Development'), ('Data Analysis'), ('Content Strategy'), ('SEO'), ('Talent Acquisition'), ('Management');

-- Наповнення Look-up таблиці Managers наявними іменами з джерел
INSERT INTO Managers (Name) VALUES
('Marcus Johnson'),
('Emily Chen'),
('Robert Jenkins'),
('Amanda Waller'),
('Steven Wong');