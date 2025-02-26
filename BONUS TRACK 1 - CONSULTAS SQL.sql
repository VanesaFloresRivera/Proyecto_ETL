-- Una vez creada la base de datos, realiza consultas a esa base de datos:

--  Cuantos hoteles tiene la base de datos
SELECT count(id_hotel) AS Num_hoteles
FROM hoteles h;

-- Cuantas reservas se han hecho
SELECT count(id_reserva) AS Num_reservas
FROM reservas r;

-- Identifica los 10 clientes que más se han gastado
SELECT 
	concat(c.nombre, ' ', c.apellido) AS "Cliente",
	sum(r.precio_noche) AS "Total_gastado"
FROM clientes c 
	INNER JOIN reservas r ON c.id_cliente =r.id_cliente 
GROUP BY c.id_cliente
ORDER BY sum(r.precio_noche) DESC
LIMIT 10;

-- Identifica el hotel de la competencia y el hotel de nuestra marca que más han recaudado para esas fechas
--Hotel de la competencia:
SELECT h.nombre_hotel AS "Nombre_hotel",
	round(sum(r.precio_noche)) AS "Total_recaudado",
	h.competencia 
FROM hoteles h 
	INNER JOIN reservas r ON h.id_hotel = r.id_hotel
WHERE h.competencia = TRUE 
GROUP BY h.competencia, h.id_hotel
ORDER BY round(sum(r.precio_noche)) DESC 
LIMIT 1;

--Hotel propio:
SELECT h.nombre_hotel AS "Nombre_hotel",
	round(sum(r.precio_noche)) AS "Total_recaudado",
	h.competencia 
FROM hoteles h 
	INNER JOIN reservas r ON h.id_hotel = r.id_hotel
WHERE h.competencia = FALSE 
GROUP BY h.competencia, h.id_hotel
ORDER BY round(sum(r.precio_noche)) DESC 
LIMIT 1;


-- Identifica cuantos eventos hay
SELECT count(id_evento) AS Num_eventos
FROM eventos e;

-- Identifica el día que más reservas se han hecho para nuestro hoteles
SELECT fecha_reserva AS "Fecha_reserva",
	count(fecha_reserva) AS "Num_reservas"
FROM reservas r
INNER JOIN hoteles h ON r.id_hotel =h.id_hotel 
WHERE h.competencia = False
GROUP BY fecha_reserva
ORDER BY count(fecha_reserva) DESC 
LIMIT 1;

SELECT h.nombre_hotel, h.competencia AS "Tipo_hotel", r.fecha_reserva, count(h.nombre_hotel) AS "Num.reservas", round(sum(r.precio_noche)) AS "Total_recaudado", round(avg(r.precio_noche)) AS "Precio_medio_noche" FROM reservas r INNER JOIN hoteles h ON r.id_hotel =h.id_hotel GROUP BY h.id_hotel,r.fecha_reserva
