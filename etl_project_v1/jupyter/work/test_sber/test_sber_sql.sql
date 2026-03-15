-- Задание 1

SELECT 
           c.customer_name     AS "Имя клиента",
       SUM(o.order_total)      AS "Сумма заказов"
FROM       test_sber.customers AS c 
INNER JOIN test_sber.orders    AS o 
        ON c.customer_id = o.customer_id 
       AND c.age > 30
GROUP BY   c.customer_name 
;


-- Задание 2

SELECT
         uo.user_id             AS "Пользователь",
         COUNT(1)               AS "Количество заказов"
FROM     test_sber.users_orders AS uo 
GROUP BY uo.user_id
HAVING   COUNT(1) > 3
ORDER BY 2 DESC
;


-- Задание 3

-- Сначала найдем список категорий у которых средний прайс выше чем у электроники
WITH avg_pr AS (
                SELECT   category
                FROM     test_sber.products AS p
                GROUP BY category
                HAVING   AVG(price) > (SELECT AVG(price) FROM test_sber.products WHERE category = 'Electronics')
               )
-- Затем используем результат CTE как фильтр и выведем список товаров (id  в нашем случае)
SELECT     p.product_id       AS "Товар"
FROM       test_sber.products AS p 
INNER JOIN avg_pr             AS ap ON p.category = ap.category 
;


-- Можно еще сделать так, но IN будет работать медленнее
-- Да и читать это глаза сломаешь
SELECT p.product_id       AS "Товар"
FROM   test_sber.products AS p
WHERE  p.category IN (
                     SELECT category
                     FROM test_sber.products
                     GROUP BY category
                     HAVING AVG(price) > (
                                          SELECT AVG(price) 
                                          FROM test_sber.products 
                                          WHERE category = 'Electronics'
                                         )
                    )
;




-- Задание 4

-- Тут насколько я понял нужен именно список сотрудников, а не просто средняя ЗП в отделе, 
-- соответственно юзаем оконные функции

SELECT 
     employee_id                           AS "Сотрудник",
     department                            AS "Отдел",
 AVG(salary) OVER(PARTITION BY department) AS "Средняя ЗП в данном отделе"
FROM test_sber.employees



-- Задание 5

SELECT 
      good_id,
      name,
      price,
      CASE
          WHEN price < 10                  THEN 'Дешевые'
          WHEN price >= 10 AND price <= 50 THEN 'Средняя стоимость'
          WHEN price > 50                  THEN 'Дорогие'
          -- больше 50 лучше прописать явно вместо ELSE, 
          -- иначе можем классифицировать NULL значения как "Дорогие", что некорректно
      END AS "category"
FROM test_sber.goods


-- Задание 6

SELECT 
     client_id                        AS "Клиент",
     CURRENT_DATE                     AS "Сегодня",
     registration_date                AS "Дата регистрации",
     -- Для Postgres можно просто вычесть, в MSSQL отдельная функция
     CURRENT_DATE - registration_date AS "Разница в днях"
FROM test_sber.clients 



-- Задание 7

-- Тут расплывчатая формулировка, может трактоваться по разному
-- Мы ищем именно факт роста, процентное изменение, или абсолютное изменение продаж
-- Тут прям напрашивается уточнение ТЗ
-- В общем сделал так и так
SELECT 
                           product_id,
                           sale_month,
                           revenue,
                       LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) AS "revenue_lag1",
             revenue - LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) AS "revenue_change_lag1",
(revenue - LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month)) / 
           LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) * 100       AS "percent",
    CASE
        WHEN           LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) IS NULL THEN 'Нет данных'
        WHEN revenue - LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) > 0     THEN 'Рост'
        WHEN revenue - LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) = 0     THEN 'Плато'
        WHEN revenue - LAG(revenue, 1) OVER(PARTITION BY product_id ORDER BY sale_month) < 0     THEN 'Падение'
    END                                                                                  AS "grow_status"
FROM test_sber.sales
ORDER BY 1,2

