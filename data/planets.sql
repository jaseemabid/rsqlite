CREATE TABLE planets (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    diameter INTEGER NOT NULL,
    distance INTEGER NOT NULL,
    moons INTEGER NOT NULL
);

INSERT INTO planets (id, name, type, diameter, distance, moons)
VALUES
(1, 'Mercury', 'Terrestrial', 4879, 57910000, 0),
(2, 'Venus', 'Terrestrial', 12104, 108200000, 0),
(3, 'Earth', 'Terrestrial', 12742, 149600000, 1),
(4, 'Mars', 'Terrestrial', 6779, 227900000, 2),
(5, 'Jupiter', 'Gas Giant', 139820, 778500000, 79),
(6, 'Saturn', 'Gas Giant', 116460, 1433000000, 83),
(7, 'Uranus', 'Ice Giant', 50724, 2871000000, 27),
(8, 'Neptune', 'Ice Giant', 49244, 4495000000, 14);