--1.- How many reviews were done in California, NY and Texas?
--California count: 2,060
SELECT COUNT(*) FROM review_logs WHERE location = 'California';
-- New York count: 1,961
SELECT COUNT(*) FROM review_logs WHERE location = 'New York';
--Texas count: 2,017
SELECT COUNT(*) FROM review_logs WHERE location = 'Texas';



--2.- How many reviews were done in California, NY and Texas with an Apple device?
--Apple
--California with Apple device count: 20,656
SELECT COUNT(*) FROM review_logs WHERE location = 'California' AND os = 'Apple iOS' OR os = 'Apple MacOS';

--New York with Apple device count: 20,639
SELECT COUNT(*) FROM review_logs WHERE location = 'New York' AND os = 'Apple iOS' OR os = 'Apple MacOS';

--Texas with Apple device count: 20,616
SELECT COUNT(*) FROM review_logs WHERE location = 'Texas' AND os = 'Apple iOS' OR os = 'Apple MacOS';

--and how many for each device type?
--Windows count: 19,856
SELECT COUNT(*) FROM review_logs WHERE os = 'Microsoft Windows';

--Linux count: 19,956
SELECT COUNT(*) FROM review_logs WHERE os = 'Linux';

--Apple count: 40,349
SELECT COUNT(*) FROM review_logs WHERE os = 'Apple iOS' OR os = 'Apple MacOS';

--Google count: 19,839
SELECT COUNT(*) FROM review_logs WHERE os = 'Google Android';



--3.- What are the states with more and fewer reviews in 2021?
--Georgia is the more reviewer with 2,104 reviews
SELECT location, COUNT(log_id) FROM review_logs GROUP BY location ORDER BY COUNT(log_id) DESC;
--Vermont is the fewer reviewer with 1,890 reviews
SELECT location, COUNT(log_id) FROM review_logs GROUP BY location ORDER BY COUNT(log_id) ASC;



--4.- Which device is the most used to write reviews in the east and which one in the west?
--EAST count: 73,786   tablet.- 24594  computer.-24537  mobile.-24655
SELECT device, COUNT(log_id)
FROM review_logs
WHERE location IN ('Alabama','Arkansas','Connecticut','Delaware','Florida','Georgia','Illinois','Indiana',
      'Iowa','Kentucky','Kansas','Lousiana','Maine','Maryland','Massachussets','Michigan','Minnesota','Mississippi','Missouri'
      ,'New Hampshire','New Jersey','New York','North Carolina','North Dakota','Ohio','Oklahoma','Pensylvania','Rhode Island','South Carolina','South Dakota','Nebraska'
      ,'Tennessee','Texas','Vermont','Virginia','West Virginia','Wisconsin')
GROUP BY device;

--WEST count 26,214 tablet.- 8,689  computer.-8,663  mobile.-8862
SELECT device, COUNT(log_id)
FROM review_logs
WHERE location IN ('Alaska','Arizona','California','Colorado','Hawaii','Idaho','Montana'
      ,'Nevada','New Mexico','Oregon','Utah'
      ,'Washington','Wyoming')
GROUP BY device;
      