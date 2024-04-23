/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT t1.name AS category_name, t2.count_films AS count_films
FROM category t1
JOIN(
    SELECT category_id, COUNT(film_id) AS count_films
    FROM film_category
    GROUP BY category_id) t2 ON t1.category_id = t2.category_id
ORDER BY count_films DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

SELECT t1.first_name, t1.last_name
FROM actor t1 RIGHT JOIN (
    SELECT t2.actor_id, count(t4.rental_id) AS count_rents
    FROM film_actor t2
    LEFT JOIN inventory t3 ON t2.film_id = t3.film_id
    LEFT JOIN rental t4 ON t3.inventory_id = t4.inventory_id
    GROUP BY t2.actor_id
    ) t5 ON t1.actor_id = t5.actor_id
ORDER BY t5.count_rents DESC
LIMIT 10;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT t7.name FROM (
    SELECT t4.name, SUM(t5.amount) AS rent_sum
    FROM rental t1
        JOIN inventory t2 ON t1.inventory_id = t2.inventory_id
        JOIN film_category t3 ON t2.film_id = t3.film_id
        JOIN category t4 ON t3.category_id = t4.category_id
        JOIN payment t5 ON t5.rental_id = t1.rental_id
    GROUP BY t4.name
    ORDER BY rent_sum DESC
    LIMIT 1) t7;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT
    t1.title AS film_name
FROM film AS t1
LEFT JOIN public.inventory AS t2 ON t1.film_id = t2.film_id
WHERE t2.film_id IS NULL
ORDER BY film_name;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

-- films_per_actor is CTE for count films per actor in category Children
WITH films_per_actor AS (
SELECT t1.actor_id, count(t1.film_id) AS num_films
FROM film_actor t1 INNER JOIN film_category t2 ON t1.film_id = t2.film_id
WHERE category_id =(
    SELECT category_id
    FROM category
    WHERE name = 'Children'
    )
GROUP BY t1.actor_id
)

SELECT first_name, last_name
FROM films_per_actor
LEFT JOIN actor ON films_per_actor.actor_id = actor.actor_id
ORDER BY films_per_actor.num_films DESC
LIMIT 3;
