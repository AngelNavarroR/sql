
CREATE OR REPLACE FUNCTION catastro.sp_valor_m2_urbano(id_predio bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE valor_m2 NUMERIC;
DECLARE band_eje BOOLEAN;
DECLARE clave_catastral VARCHAR;
DECLARE px RECORD;
DECLARE count_ integer;
BEGIN

--Consulta si el predio esta en eje comercial
	SELECT clave_cat, area_solar INTO px FROM catastro.cat_predio WHERE id = id_predio;
	SELECT COUNT(*) INTO count_ FROM geodata.predios_tx  WHERE codigo = px.clave_cat;
   band_eje=FALSE;
   valor_m2=(SELECT sv.valor FROM geodata.predios_tx pt 
				INNER JOIN geodata.ct_ejes_valor sv ON st_intersects(pt.geom, St_Buffer(sv.geom, sv.buffer)) 
				WHERE pt.codigo = px.clave_cat GROUP BY 1);    	
   IF valor_m2>0 THEN
		band_eje=TRUE;
   END IF;  
	--RAISE NOTICE 'VALOR % EN EJE % count_ %',valor_m2, band_eje, count_; 
-- Se obtiene el valor m2
    IF band_eje=FALSE THEN
			IF(count_ > 1) THEN 
				valor_m2 = COALESCE((SELECT sv.costo_de_z FROM geodata.predios_tx pt 
					INNER JOIN geodata.ct_sectores_valor sv ON st_intersects(ST_PointOnSurface(pt.geom), sv.geom) 
					WHERE pt.codigo = px.clave_cat and pt.pre_area::integer = px.area_solar GROUP BY 1), 0.00);
			ELSE 
				valor_m2 = COALESCE((SELECT sv.costo_de_z FROM geodata.predios_tx pt 
					INNER JOIN geodata.ct_sectores_valor sv ON st_intersects(ST_PointOnSurface(pt.geom), sv.geom) 
					WHERE pt.codigo = px.clave_cat GROUP BY 1), 0.00);
			END IF;
			IF valor_m2 <=0 THEN 
				RAISE NOTICE 'PREDIO % NO SE INTERCEPTA EN LA CAPA ct_sectores_valor % ', id_predio, px.clave_cat;
			END IF ;
	END IF;
     return valor_m2;
END;
$function$
;


CREATE OR REPLACE FUNCTION catastro.sp_valor_terreno_urbano(id_predio bigint, actualizar boolean)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE area_total NUMERIC;
DECLARE pubs NUMERIC;
DECLARE coef_afectacion NUMERIC;
DECLARE coef_afec_serv NUMERIC;
DECLARE coef_afec_uso_suelo NUMERIC;
DECLARE coef_afec_vias NUMERIC;
DECLARE factor_frente NUMERIC;
DECLARE factor_fondo NUMERIC;
DECLARE factor_ajuste NUMERIC;
DECLARE valor_m2_pubs NUMERIC;
DECLARE valor_terreno NUMERIC;
DECLARE valor_construccion NUMERIC;
DECLARE intermedio INTEGER;

BEGIN
    -- Se obtiene el area del Predio
       area_total=(SELECT p.area_solar FROM catastro.cat_predio p WHERE p.id = id_predio AND estado IN ('A', 'P'));
    -- Se obtiene el valor m2
       valor_m2_pubs=COALESCE((select catastro.sp_valor_m2_urbano(id_predio)), 0.00);
    -- Se obtiene el coeficiente de afectacion servicios basicos
      coef_afec_serv = (SELECT ROUND(COALESCE(abast_agua.factor, 0) * COALESCE(elimi_excre.factor, 0) * COALESCE(energ_elct.factor, 0) * 
			(CASE WHEN s6.aseo_calles = TRUE THEN 1.00 ELSE 0.970 END) * COALESCE(recol_basu.factor, 0) *  
			(CASE WHEN s6.alumbrado = TRUE THEN 1.00 ELSE 0.940 END) * COALESCE(ag_recibe.factor, 0) * 
			(CASE WHEN s6.tpublico = TRUE THEN 1.000 ELSE 1.000 END) * (CASE WHEN s6.telefonia_satelital = TRUE THEN 0.960 ELSE 1.000 END),4)
			FROM catastro.cat_predio p 
			LEFT JOIN catastro.cat_predio_s6 s6 ON s6.predio = p.id 
			LEFT JOIN catastro.ctlg_item abast_agua ON abast_agua.id = s6.abast_agua_proviene
			LEFT JOIN catastro.ctlg_item elimi_excre ON elimi_excre.id = s6.evac_aguas_serv
			LEFT JOIN catastro.ctlg_item energ_elct ON energ_elct.id = s6.abaste_electrico
			LEFT JOIN catastro.ctlg_item recol_basu ON recol_basu.id = s6.recol_basura
			LEFT JOIN catastro.ctlg_item ag_recibe ON ag_recibe.id = s6.abas_agua_recibe
			WHERE p.id = id_predio AND p.estado IN ('A', 'P'));
	-- Se obtiene el coeficiente de afectacion del suelo
        coef_afectacion=1;
        coef_afectacion=COALESCE((SELECT 
				round(nt.factor*fs.factor*ts.factor*i.factor*tos.factor*CASE WHEN rg.factor IS NULL THEN 1.0000 ELSE rg.factor END
				*(CASE WHEN s4.tiene_permiso_const = TRUE THEN 1 ELSE 1 END) * (CASE WHEN s4.tiene_adosamiento = TRUE THEN 1 ELSE 1 END),2)
				FROM catastro.cat_predio p
					INNER JOIN catastro.cat_predio_s4 s4 ON s4.predio=p.id
					LEFT OUTER JOIN catastro.ctlg_item nt ON nt.id=s4.nivel_terreno 
					LEFT OUTER JOIN catastro.ctlg_item fs ON fs.id=p.forma_solar
					LEFT OUTER JOIN catastro.ctlg_item ts ON ts.id=p.tipo_suelo
					LEFT OUTER JOIN catastro.ctlg_item i ON i.id=s4.loc_manzana
					LEFT OUTER JOIN catastro.ctlg_item tos ON tos.id=p.topografia_solar
					LEFT OUTER JOIN catastro.ctlg_item rg ON tos.id=s4.riesgo
				WHERE p.estado IN ('A', 'P') AND p.id = id_predio), 1);
    -- Coeficiente de Uso del suelo
		coef_afec_uso_suelo = ROUND(COALESCE((SELECT avg(usop.factor) FROM catastro.cat_predio p 
			INNER JOIN catastro.cat_predio_s6 s6 ON s6.predio = p.id
			INNER JOIN catastro.cat_predio_s6_has_usos s6uso ON s6uso.predio_s6 = s6.id
			LEFT OUTER JOIN catastro.ctlg_item usop ON usop.id=s6uso.uso 
			WHERE p.id = id_predio AND p.estado IN ('A', 'P')), 1),4);
	-- Coeficiente de Uso de via
		coef_afec_vias = ROUND(COALESCE((SELECT COALESCE(tv.factor, 1)*COALESCE(mv.factor, 0.830)
			*(CASE WHEN s6.tiene_aceras = TRUE THEN 1 ELSE 0.930 END) * (CASE WHEN s6.tiene_bordillo = TRUE THEN 1 ELSE 0.950 END)
			FROM catastro.cat_predio p 
			INNER JOIN catastro.cat_predio_s4 s4 ON s4.predio = p.id
			INNER JOIN catastro.cat_predio_s6 s6 ON s6.predio = s4.predio
			LEFT OUTER JOIN catastro.ctlg_item tv ON p.tipo_via=tv.id 
			LEFT OUTER JOIN catastro.ctlg_item mv ON s4.rodadura=mv.id 
			WHERE p.id = id_predio AND p.estado IN ('A', 'P')), 1), 4);
       -- Calcuar valor tereno predio
	-- FACTOR FRENTE
		factor_frente = COALESCE((SELECT factor 
			FROM catastro.ctlg_item WHERE catalogo = (SELECT id FROM catastro.ctlg_catalogo WHERE nombre = 'Coeficiente Frente') 
			AND (SELECT s4.frente1 FROM catastro.cat_predio p INNER JOIN catastro.cat_predio_s4 s4 ON s4.predio = p.id
			WHERE p.id = id_predio AND p.estado IN ('A', 'P')) BETWEEN rango_desde AND rango_hasta), 1);
	-- FACTOR FONDO
		factor_fondo = COALESCE((SELECT factor 
			FROM catastro.ctlg_item WHERE catalogo = (SELECT id FROM catastro.ctlg_catalogo WHERE nombre = 'Coeficiente Fondo') 
			AND (SELECT s4.fondo1 FROM catastro.cat_predio p INNER JOIN catastro.cat_predio_s4 s4 ON s4.predio = p.id
			WHERE p.id = id_predio AND p.estado IN ('A', 'P')) BETWEEN rango_desde AND rango_hasta), 1);
	-- FACTOR AJUSTE POR TAMANIO
		factor_ajuste = COALESCE((SELECT factor 
			FROM catastro.ctlg_item WHERE catalogo = (SELECT id FROM catastro.ctlg_catalogo WHERE nombre = 'AJUSTE_POR_TAMANIO') 
			AND area_total BETWEEN rango_desde AND rango_hasta), 1);
	IF valor_m2_pubs = 0 THEN 
	   RAISE NOTICE 'id_predio % AREA % PUBS % COEF_SERV_BAS % COEF_AFEC %, CEOF_SUELO % CEOF_VIAS % FACT_FRENTE % FACT_FONDO % FACT_AJUSTE %'
	  			,id_predio , area_total, valor_m2_pubs, coef_afec_serv, coef_afectacion, coef_afec_uso_suelo,coef_afec_vias, factor_frente, factor_fondo, factor_ajuste;
	END IF;
    -- CALCULAMOS EL VALOR DEL PREDIO
	valor_terreno=coalesce(round(area_total*valor_m2_pubs*coef_afec_serv*coef_afectacion*coef_afec_uso_suelo*coef_afec_vias*factor_frente*factor_fondo*factor_ajuste,2), 0.00);
	-- actualizar el valor del predio
	IF actualizar = true THEN 
		valor_construccion = catastro.sp_valor_edificacion(id_predio, null, TRUE);
		UPDATE catastro.cat_predio SET avaluo_solar = valor_terreno, avaluo_municipal = COALESCE(valor_construccion, 0.0) + valor_terreno,
		avaluo_construccion = COALESCE(valor_construccion, 0.0), base_imponible = COALESCE(valor_construccion, 0.0) + valor_terreno WHERE id = id_predio;
	ELSE 
		UPDATE catastro.cat_predio SET avaluo_solar = valor_terreno, avaluo_municipal = coalesce(avaluo_construccion, 0.00) + valor_terreno,
		avaluo_construccion = coalesce(avaluo_construccion, 0.00), base_imponible = coalesce(avaluo_construccion, 0.00) + valor_terreno WHERE id = id_predio;
	END IF;	
	
	IF (SELECT p.ficha_madre FROM catastro.cat_predio p WHERE p.id = id_predio AND estado IN ('A', 'P')) = true THEN 
		WITH CD AS (
			SELECT p.id, p.alicuota_const, p.predio_raiz, p.num_predio,
			m.area_solar, (m.area_solar*p.alicuota_const)/100 area_solar_p,
			m.area_Declarada_Const, (COALESCE(m.area_Declarada_Const, 0)*p.alicuota_const)/100 area_declarada_const_p,
			m.avaluo_solar, (COALESCE(m.avaluo_solar, 0)*p.alicuota_const)/100 avaluo_solar_p,
			m.avaluo_construccion, (COALESCE(m.avaluo_construccion, 0)*p.alicuota_const)/100 avaluo_construccion_p,
			p.area_Declarada_Const, p.avaluo_construccion, p.clave_cat,
			p.avaluo_solar
			FROM catastro.cat_predio p 
			INNER JOIN catastro.cat_predio m
			ON m.id=P.predio_raiz 
			WHERE p.propiedad_horizontal=true
			AND m.id=id_predio
		) UPDATE catastro.cat_predio f SET area_solar=cd.area_solar_p,
		area_declarada_const=cd.area_declarada_const_p,
		avaluo_solar=cd.avaluo_solar_p,
		avaluo_construccion=cd.avaluo_construccion_p,
		base_imponible=cd.avaluo_solar_p+cd.avaluo_construccion_p,
		avaluo_municipal=cd.avaluo_solar_p+cd.avaluo_construccion_p 
		FROM CD WHERE cd.id=f.id ;
	END IF;
	return valor_terreno;   
END;
$function$
;


CREATE OR REPLACE FUNCTION catastro.sp_valorar_todos_terreno_urbano(usuario_valoracion character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE count_ INTEGER;
DECLARE avaluo_s NUMERIC;
DECLARE avaluo_cons NUMERIC;
DECLARE predio_cat RECORD;
BEGIN
	--TRUNCATE TABLE valoracion.avaluo_predio RESTART IDENTITY;
	--TRUNCATE TABLE valoracion.avaluo_predio_bloque RESTART IDENTITY;
--VALORA TODOS LOS PREIOS DEL CANTON
	count_ = 0;
	FOR predio_cat IN
			SELECT clave_cat, id, avaluo_construccion, area_solar, predialant FROM catastro.cat_predio WHERE estado IN ('A', 'P') ORDER BY clave_cat ASC 
		LOOP
			UPDATE valoracion.avaluo_predio  SET estado = false where predio = predio_cat.id AND estado = TRUE;
			-- AVALUO SOLAR
			avaluo_s = catastro.sp_valor_terreno_urbano(predio_cat.id, FALSE);
			-- AVALUO CONSTRUCCION
			avaluo_cons = catastro.sp_valor_edificacion(predio_cat.id, NULL, TRUE);
			INSERT INTO valoracion.avaluo_predio(abast_agua_fact, elimi_excre_fact, energ_elct_fact, recol_basu_fact, ag_recibe_fact, aseo_calles, alumbrado, 
												 trans_publico, telefonia_satelital, nivel_terreno, forma_solar, tipo_suelo, loc_manzana, 
												 topografia_solar, riesgo, predio, clave_cat, permiso_cons, adosamiento, uso_suelo_fact, 
												 tipo_via_fact, mate_via_fact, aceras_fact, bordillo_fact, area_solar, avaluo_solar, avaluo_construccion, 
												 fecha_valoracion, usuario_valoracion, clave_ant)
			SELECT v.*, predio_cat.area_solar, avaluo_s, avaluo_cons, now(), usuario_valoracion, predio_cat.predialant  
			FROM catastro.parametros_valoracion_predio_all v WHERE v.predio = predio_cat.id;
			UPDATE catastro.cat_predio SET avaluo_solar = avaluo_s, avaluo_municipal = coalesce(avaluo_cons, 0) + coalesce(avaluo_s, 0.00),
			avaluo_construccion = avaluo_cons, base_imponible = coalesce(avaluo_cons, 0) + avaluo_s WHERE id = predio_cat.id;
			count_ = count_ + 1;
    END LOOP;
   	RAISE NOTICE '** FIN DEL PROCESO DE VALORACION';
	return count_;
END;
$function$
;

CREATE OR REPLACE FUNCTION catastro.calculo_impuesto_predial(_predio_id bigint, usuario text, id_aval_impuesto bigint, avaluo_municipal numeric, anio_inicio_val integer, anio_fin_val integer, considera_construccion boolean)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
<<fn>>
	DECLARE
	aval_impuesto_predios   RECORD;				ubicaciones             	RECORD;
	ubicacion_predio        BIGINT :=0;			es_predio_marginal      	BOOLEAN := FALSE;
	obra_ubicacion          RECORD;				banda                   	RECORD;
	impuesto_predial        numeric :=0.00;		solar_no_edif           	numeric := 0.0;
	tasa_seg   				numeric := 0.0;		calculo_bombero         	numeric := 0.0;
	calculo_mejora          numeric := 0.0;		calculo_mejora1         	numeric := 0.0;
	calculo_mejora2         numeric := 0.0;		calculo_mejora3         	numeric := 0.0;
	calculo_mejora4         numeric := 0.0;		calculo_mejora_tola     	numeric := 0.0;
	tasa_administrativa     numeric := 0.0;		valor_tasa_administrativa  	numeric := 0.0;
	resut                   numeric := 0.0;		resut_total_emitido     	numeric := 0.0;
	valor_banda             numeric := 0.0;		aval_det_cobro_impuesto_predios RECORD;
	predio                  RECORD;				remuneraciones_salariales_25 numeric (19,2) := 0.00;
	contador                bigint := 0;		max_num_liquidacion			bigint := 0;
	data_					record;				data_sal					record;
	BEGIN
		SELECT * into data_sal FROM catastro.ctlg_salario cs where anio = anio_fin_val;
		if data_sal.id is null then 
			INSERT INTO catastro.ctlg_salario (anio,valor) VALUES (anio_fin_val,460.00);
		end if;
		SELECT p.id, p.avaluo_municipal,  p.propiedad 
			INTO predio FROM catastro.cat_predio p WHERE id = _predio_id;
		SELECT * INTO banda FROM catastro.aval_banda_impositiva cfc1 WHERE coalesce(predio.avaluo_municipal, 0.00) BETWEEN cfc1.desde_us AND cfc1.hasta_us;
		IF(predio.propiedad is null) THEN predio.propiedad = 3840; END IF;
		SELECT * into data_sal FROM catastro.ctlg_salario cs where anio = anio_fin_val;
		remuneraciones_salariales_25  := (data_sal.valor * 25);
		raise notice 'Salario %', data_sal.valor;
		-- SELECT * FROM sgm_financiero.ren_rubros_liquidacion WHERE id in(3, 7, 988, 8, 640, 986, 987, 989)
		for data_ in SELECT id, valor, estado FROM sgm_financiero.ren_rubros_liquidacion WHERE id in(3, 7, 988, 8, 640, 986, 987, 989) loop 
				IF(predio.propiedad NOT IN (3839))THEN 
				---TASAS ADMINISRATIVAS RUBRO 3
				IF data_.id = 3 AND data_.estado = true THEN 
					--SELECT * FROM catastro.ctlg_salario cs;
					-- Calculamos el valor de tasa administrativa
					valor_tasa_administrativa := round((data_sal.valor * 0.003), 2); 
				if valor_tasa_administrativa = 0 then 
					valor_tasa_administrativa = 1.38;
				end if;
				END IF;
				---TASA DE SEGURIDAD 7
				IF data_.id = 988 AND data_.estado = true THEN
					tasa_seg := data_.valor;
				END IF;
				---COBRO DE BOMBEROS  :D RUBRO 7
				IF data_.id = 7 AND data_.estado = true THEN
					calculo_bombero := round((0.15 * predio.avaluo_municipal) / 1000, 2);
				END IF;
				----MEJORA 1 ==> CEM Reg. Urb. 
				IF data_.id = 8 AND data_.estado = true THEN
					calculo_mejora := data_.valor;
				END IF;
				---CEM2 ==> CEM PARQUE 
				IF data_.id = 640 AND data_.estado = true THEN
					calculo_mejora1 := data_.valor;
				END IF;
				---CEM3 ==> CEM ASFALTO 
				IF data_.id = 986 AND data_.estado = true THEN
					calculo_mejora2 := data_.valor;
				END IF;
				---CEM4 ==> CEM POZ. EMIL 
				IF data_.id = 989 AND data_.estado = true THEN
					--calculo_mejora4 := data_.valor;
					SELECT d.valor into calculo_mejora4 FROM sgm_financiero.ren_liquidacion l
						INNER JOIN sgm_financiero.ren_det_liquidacion d ON d.liquidacion = l.id 
						WHERE d.rubro = data_.id AND l.anio = 2023 and l.predio = predio.id;
				END IF;
				--- CEM5==> CEM MALECON 
				IF data_.id = 987 AND data_.estado = true THEN
					IF data_.valor > 0 THEN 
						calculo_mejora3 := data_.valor;
					ELSE 
						SELECT d.valor INTO calculo_mejora3 FROM sgm_financiero.ren_liquidacion l
						INNER JOIN sgm_financiero.ren_det_liquidacion d ON d.liquidacion = l.id 
						WHERE d.rubro = data_.id AND l.anio = 2023 and l.predio = predio.id;
						--WHERE d.rubro = 987 AND l.anio = 2023 and l.predio = 22726; 
					END IF;
				END IF;
			END IF;
		end loop;
		
		---IMPUESTO PREDIO URBANO (EXCLUYE A LOS PREDIOS DE TENENCIA MUNICIPAL - PUBLICA Y ESTADO )
		IF(predio.propiedad NOT IN (3839))THEN 
			IF predio.avaluo_municipal > remuneraciones_salariales_25 THEN  
				impuesto_predial := ((predio.avaluo_municipal * banda.multiplo_impuesto_predial) / 1000);
			END IF;
		END IF;
			
		IF (impuesto_predial IS NULL ) THEN impuesto_predial:= 0; END IF;
								
		resut := catastro.save_ren_liquidacion(_predio_id, usuario, round(avaluo_municipal ,2), round(impuesto_predial,2), 
				round(tasa_seg,2), round(calculo_bombero,2), round(calculo_mejora,2), anio_inicio_val, anio_fin_val, 
				valor_banda, round(calculo_mejora1,2), round(valor_tasa_administrativa,2 ), considera_construccion, 
				round(calculo_mejora2,2), round(calculo_mejora3,2), round(calculo_mejora4,2));
		resut_total_emitido = resut_total_emitido + resut;
		IF resut_total_emitido <= 0 THEN 
			RAISE NOTICE 'PREDIO % IMPUESTO MUNICIPAL % BANDA IMPOSITIVA %', predio.id, resut, banda;
		END IF;
		---- AQUI SE CALCULAN LOS COBROS DE LAS MEJORAS SI ES QUE EL PREDIO SE ENCUENTRA EN LA UBICACION DADA :V
		--PRIMERO BUSCA SI EL  PREDIO ESTA DENTRO D LA TABLA CAT_UBICACION ESTA ASU VES ESTA RELACIONADA CON 
		--VALORES OBRA UBICACION DEL SGM_MEJORA
		--PARA QUE SE COMPARE SI SE LE COBRARA O NO UA MEJORA AL PREDIO =O
		/*FOR obra_ubicacion IN ( SELECT ou.id  as id_valor_obra_ubicacion, o.tipo_obra, ou.valor_recuperar, ou.id, o.rubro, ou.ubicacion
			FROM sgm_mejoras.mej_valores_obra_ubicacion ou INNER JOIN sgm_mejoras.mej_obra o ON (ou.obra=o.id) WHERE o.anio=anio_inicio_val)
		LOOP
			ubicacion_predio := sgm_mejoras.get_ubicacion_predio(_predio_id, obra_ubicacion.ubicacion); 
			---SI LA UBICACION ES MAYOR A CERO SIGNIFICA QUE ESE PREDIO TIENE UNA MEJORA
			IF (ubicacion_predio > 0 ) THEN
				IF((SELECT count(*) FROM catastro.aval_det_cobro_impuesto_predios cobro  
					WHERE  cobro.id_aval_impuesto_predio = aval_impuesto_predios.id 
					AND cobro.id_rubro_cobrar = obra_ubicacion.rubro) = 1) THEN 
						PERFORM sgm_mejoras.recorrer_avaluos_emision_predial(_predio_id, anio_inicio_val, ubicacion_predio, obra_ubicacion.id_valor_obra_ubicacion);
				END IF;	
			END IF;
		END LOOP;
*/
		
	END
$function$
;


CREATE OR REPLACE FUNCTION catastro.emision_predial_ren_liquidacion_anual(_predio_id bigint, usuario text, anio_inicio_val integer, anio_fin_val integer, considera_construccion boolean)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
<<fn>>
	DECLARE
		predio RECORD;
		resp text;
		avaluo_anterior RECORD;
		anio_anterior integer;
		ACIERTOS BIGINT := 0;
		
	declare contador1 integer;
	declare emision cursor for select * from sgm_financiero.ren_liquidacion l where l.anio = (anio_fin_val - 1) and l.tipo_liquidacion = 303;
	declare em record;
	declare em_det record;
	DECLARE cem_migra BIGINT;

	BEGIN
	--- OBTIENE EL AÑO ANTERIOR PARA CUANDO SE CALCULE A UNO DIFERENTE AL ACTUAL
	 anio_anterior := (select extract(year from now())::INTEGER)  - 1;
	ACIERTOS = 0;
	--PREDIO
	--SE LE ENVIA CERO CUANDO SSE VAN A EVALUAR TODOS LOS PREDIOS
		IF (_predio_id = 0) THEN 
			FOR predio IN SELECT id, avaluo_construccion, avaluo_solar, p.avaluo_municipal, tipo_predio, num_predio    
				FROM catastro.cat_predio p WHERE estado = 'A' AND tipo_predio = 'U' ORDER BY num_predio ASC LOOP
				ACIERTOS := ACIERTOS + 1;
				-- UPDATE sgm_financiero.ren_liquidacion set estado_liquidacion = 3  WHERE sgm_financiero.ren_liquidacion.anio = anio_inicio_val and sgm_financiero.ren_liquidacion.predio = predio.id;
				resp := catastro.calculo_impuesto_predial(predio.id, usuario, 1, predio.avaluo_construccion, anio_inicio_val, anio_fin_val,  considera_construccion );
				PERFORM catastro.emision_predial_predio_exonerado(predio.id, anio_inicio_val);
				-- RAISE NOTICE 'ACIERTOS %', ACIERTOS;
				-- catastro.insert_values_iniciales_liquidaciones(usuario, anio_inicio_val);
				if (ACIERTOS % 100) = 0 then 
					RAISE NOTICE 'Procesados %', ACIERTOS;
				end if;
			END LOOP;
		with s as (
			select sum(d.valor) total_s, d.liquidacion from sgm_financiero.ren_liquidacion l inner join sgm_financiero.ren_det_liquidacion d on l.id = d.liquidacion 
			where l.anio = anio_inicio_val and l.tipo_liquidacion = 13 group by 2
		) update sgm_financiero.ren_liquidacion lx set total_pago = s.total_s, saldo = s.total_s from s where s.liquidacion=lx.id;
		-- **************************
		-- COPIA DE CEM 303
		-- **************************
		contador1=0;
		FOR em  in emision LOOP
			INSERT INTO sgm_financiero.ren_liquidacion(tramite, num_comprobante, num_liquidacion, id_liquidacion, tipo_liquidacion, 
													   total_pago, usuario_ingreso, fecha_ingreso, comprador, vendedor, costo_adq, 
													   cuantia, fecha_contrato_ant, saldo, estado_liquidacion, coactiva, codigo_local, 
													   predio, local_comercial, observacion, anio, valor_comercial, valor_catastral, 
													   valor_hipoteca, valor_nominal, valor_mora, total_adicionales, otros, valor_compra, 
													   valor_venta, valor_mejoras, area_total, patrimonio, num_reporte, avaluo_municipal, 
													   avaluo_construccion, avaluo_solar, bombero, categoria_predio, predio_rustico, 
													   estado_coactiva, exonerado, nombre_comprador, nombre_comprador_historic, 
													   banda_impositiva, rural_excel, banda_temp_2, nombre_comprador_respaldo, 
													   migrado, estado_referencia, predio_migrado, predio_historico, identificacion, 
													   temporal, valor_exoneracion, exoneracion_descripcion, porciento_rebaja, validada, 
													   fecha_creacion_original, base_imponible, usuario_valida, patente, ubicacion, 
													   vendedor_rural, vendedor_rural_identificacion, clave_catastral_rural, 
													   departamento_detalle_titulos, usuario_anular, convenio_pago, fecha_anulacion,
													   mac_addres_usuario_ingreso, ip_user_session, cuenta_agua, t_cartera, t_titulo, rentas)
				values(em.tramite, em.num_comprobante, em.num_liquidacion, em.id_liquidacion, em.tipo_liquidacion, em.total_pago, em.usuario_ingreso, 
					now(), em.comprador, em.vendedor, em.costo_adq, em.cuantia, em.fecha_contrato_ant, em.total_pago, 
					2, em.coactiva, em.codigo_local, em.predio, em.local_comercial, em.observacion, 
					anio_fin_val, em.valor_comercial, em.valor_catastral, em.valor_hipoteca, em.valor_nominal, em.valor_mora, 
					em.total_adicionales, em.otros, em.valor_compra, em.valor_venta, em.valor_mejoras, em.area_total, 
					em.patrimonio, em.num_reporte, em.avaluo_municipal, em.avaluo_construccion, em.avaluo_solar, em.bombero, 
					em.categoria_predio, em.predio_rustico, em.estado_coactiva, em.exonerado, em.nombre_comprador, 
					em.nombre_comprador_historic, em.banda_impositiva, em.rural_excel, em.banda_temp_2, em.nombre_comprador_respaldo, 
					em.migrado, em.estado_referencia, em.predio_migrado, em.predio_historico, em.identificacion, em.temporal, 
					em.valor_exoneracion, em.exoneracion_descripcion, em.porciento_rebaja, em.validada, em.fecha_creacion_original, 
					em.base_imponible, em.usuario_valida, em.patente, em.ubicacion, em.vendedor_rural, em.vendedor_rural_identificacion, 
					em.clave_catastral_rural, em.departamento_detalle_titulos, em.usuario_anular, em.convenio_pago, em.fecha_anulacion,
					em.mac_addres_usuario_ingreso, em.ip_user_session, em.cuenta_agua, em.t_cartera, em.t_titulo, em.rentas )
					RETURNING id INTO cem_migra;
		
				FOR em_det IN SELECT * FROM sgm_financiero.ren_det_liquidacion where liquidacion = em.id LOOP
					insert into sgm_financiero.ren_det_liquidacion(liquidacion,rubro,valor,estado,valor_recaudado) 
						values (cem_migra,em_det.rubro,em_det.valor,true,0.00);
				END LOOP;
				contador1 = contador1+1;
		END LOOP;
		RAISE NOTICE 'PROCESADOS MEJORAS %', contador1;
		-- **************************
		-- FIN COPIA DE CEM 303
		-- **************************
	
			-- INSERTAMOS LOS VALORES INICIALES
			INSERT INTO sgm_financiero.ren_valor_inicial_emision (rubro_liquidacion, anio, valor_inicial, estado) 
			SELECT d.rubro, l.anio, SUM(d.valor), true FROM sgm_financiero.ren_det_liquidacion d 
			INNER JOIN sgm_financiero.ren_liquidacion l ON l.id = d.liquidacion WHERE l.anio = anio_inicio_val
			GROUP BY 1,2,4;
		
		ELSE
			FOR predio IN SELECT * FROM catastro.cat_predio WHERE id = _predio_id LOOP
				--UPDATE sgm_financiero.ren_liquidacion set estado_liquidacion = 3  WHERE sgm_financiero.ren_liquidacion.anio = anio_inicio_val and sgm_financiero.ren_liquidacion.predio = predio.id;	   
				--CALCULO CON EL AVLAUO DE ANIOS ANTERIORES 
				IF(anio_inicio_val <= anio_anterior) THEN 
					SELECT * INTO avaluo_anterior FROM  catastro.emitir_predio_anio_anterior(predio.id , anio_inicio_val, anio_fin_val, usuario, predio.area_solar, predio.area_declarada_const, considera_construccion);
					IF (avaluo_anterior.avaltotal IS NOT NULL AND avaluo_anterior.avaltotal > 0 )THEN 
						predio.avaluo_municipal := avaluo_anterior.avaltotal;
						predio.avaluo_construccion := avaluo_anterior.avaledif;
						predio.avaluo_solar := avaluo_anterior.avalsolar;
					END IF;
					resp := catastro.calculo_impuesto_predial(predio.id, usuario, 1, predio.avaluo_municipal, anio_inicio_val, anio_fin_val , considera_construccion);
				ELSE
					--resp := catastro.avaluar_predio( predio.id, usuario, anio_inicio_val , anio_fin_val , false);
					resp := catastro.calculo_impuesto_predial(predio.id, usuario, 1, predio.avaluo_municipal, anio_inicio_val, anio_fin_val,  considera_construccion );
					PERFORM catastro.emision_predial_predio_exonerado(predio.id, anio_inicio_val);
				END IF;
				/*resp := catastro.avaluar_predio( predio.id, usuario, anio_inicio_val , anio_fin_val );
				resp := catastro.calculo_impuesto_predial(predio.id, usuario, 1, predio.avaluo_municipal, anio_inicio_val, anio_fin_val );	*/	
				
			END LOOP;
		END IF;
		
		RETURN 'OKI';
	END
$function$
;


/*
select * from catastro.aval_banda_impositiva x;
UPDATE catastro.aval_banda_impositiva x SET anio_fin = 2024 WHERE anio_fin=2023;
select catastro.sp_valorar_todos_terreno_urbano('ANavarro');
select catastro.emision_predial_ren_liquidacion_anual(0, 'ANavarro', 2024, 2024, true); -- 4800

UPDATE catastro.acl_user SET pass='908446335d446eed444a739a7fbd1de4c179dc5c3339198ddac1190fb3b50233f11f93dbe4ff9e980a81f35331bb661a6dcecd30a79d0473a2fb244a5cc7b5bd' WHERE id=5151701;


-- *********************************************************************** 
select catastro.sp_valor_terreno_urbano(23012, FALSE);

with s as (
	select * from sgm_financiero.ren_liquidacion where anio = 2024 
) delete from sgm_financiero.ren_det_liquidacion x using s where s.id= x.liquidacion -- 31872 

with s as (
	select * from sgm_financiero.ren_liquidacion where anio = 2024 
)delete from sgm_financiero.ren_liquidacion x using s where s.id= x.id -- 9962

*/

-- **********************************************************************************************************************************
-- MIGRACION RURAL
-- **********************************************************************************************************************************

-- DROP FUNCTION migrar_temp.insertarcatpredio();

CREATE OR REPLACE FUNCTION migrar_temp.insertarcatpredio_anual()
 RETURNS integer 
 LANGUAGE plpgsql
AS $function$
DECLARE PREDIO     BIGINT;
DECLARE ID_PREDIO     BIGINT;
DECLARE NUM_PREDIO     BIGINT;
DECLARE ID_ENTE     BIGINT;
DECLARE COUNT_     BIGINT;
DECLARE CEDULA_     Text;
DECLARE NOMBRES_     Text;
DECLARE DIRECCION_     Text;

	T_CAT_PREDIO CURSOR FOR
		SELECT distinct 
		12 AS provincia, 	12 AS canton,
		50 AS parroquia,	0 AS zona,
		0 AS sector,		0 AS mz,
		0 AS solar, 		numpre AS clave_cat, 
		idpred AS predialant, 		avaluon AS avaluo_solar,
		superf AS area_solar,		0.0000 AS area_cultivos,
		p.id, cedula,propie, ubi
		FROM migrar_temp.predio_rural r 
		LEFT JOIN catastro.cat_predio p on (p.clave_cat = r.numpre::text and p.tipo_predio = 'R' )
		where p.id is null 
		order by numpre;
	BEGIN
	COUNT_ = 0;
	FOR T IN T_CAT_PREDIO LOOP
		INSERT INTO catastro.cat_predio(
			provincia, canton, parroquia, zona, sector, mz, solar, clave_cat, predialant, avaluo_solar, area_solar, area_cultivos, inst_creacion, lote, bloque, piso, unidad, estado,num_predio,tipo_predio)
			values(T.provincia, T.canton, T.parroquia, T.zona, T.sector, T.mz, T.solar, T.clave_cat, T.predialant, T.avaluo_solar, T.area_solar, T.area_cultivos, now(), 0, 0, 0, 0,'A',(select (max(pre.num_predio)+1) from catastro.cat_predio pre),'R')
			RETURNING id INTO ID_PREDIO;
		COUNT_ = COUNT_ + 1;
		IF(ID_PREDIO is not null)THEN
			CEDULA_ := trim(T.cedula);
			NOMBRES_ := T.propie;
			DIRECCION_ := T.ubi;
			RAISE NOTICE 'LLEGO % cedula %', NOMBRES_, CEDULA_;
			CEDULA_ = replace(CEDULA_,'-','');
			IF length(CEDULA_)=9 or length(CEDULA_)=12 then
				CEDULA_ = '0'||CEDULA_;
			END IF;
			ID_ENTE := (select max(ce.id) from catastro.cat_ente ce where trim(ce.ci_ruc) = CEDULA_);
			IF ID_ENTE is null then
				ID_ENTE := (select max(ce.id) from catastro.cat_ente ce where (coalesce(trim(ce.apellidos), '') || ' ' || coalesce(trim(ce.nombres), '')) = NOMBRES_);
			END IF;
			IF ID_ENTE is null then
				ID_ENTE := migrar_temp.patentes_migracion(NOMBRES_, DIRECCION_, CEDULA_, TRUE);
			END IF;
			INSERT INTO catastro.cat_predio_propietario(
			ente, predio, tipo, es_residente, estado, usuario, ciu_ced_ruc, nombres_completos,porcentaje_posecion)
			values(ID_ENTE,ID_PREDIO,60,true,'A','admin',CEDULA_,NOMBRES_,100);
		END IF;
	END LOOP;
	RETURN COUNT_;
END;
$function$
;


CREATE OR REPLACE FUNCTION migrar_temp.migrarprediorural_anual()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE comprador            BIGINT;		DECLARE ID_REN_LIQUIDACION   BIGINT;
DECLARE ID_PAGO              BIGINT;		DECLARE bomberoexist         BOOLEAN;
DECLARE rubro                BIGINT;		DECLARE estadoLiquidacion    BIGINT;
DECLARE tipoLiquidacion      BIGINT;		DECLARE observacion          TEXT;
DECLARE num_cedula           TEXT;			DECLARE nombre_compra        TEXT;
DECLARE estado_pago          BOOLEAN;		DECLARE exonera	             BOOLEAN;
DECLARE saldo                DECIMAL;		DECLARE predioUrbano         BIGINT;   
DECLARE predioRural          BIGINT;		DECLARE count_ 				 BIGINT;
DECLARE 
--CURSORES
   T_TITULO_PREDIAL CURSOR FOR
   	SELECT 
		ROW_NUMBER() OVER(PARTITION BY pr.anio ORDER BY pr.anio, numpre::integer) num_liquidacion,
	    superf			AS area_total, 			numpre 			AS pre_codigo_catastral, 
		idpred       	AS num_titulo,			avaluon 		AS valor_terr_predio,  
		0		   		AS valor_edif_predio,	avaluon       	AS val_comer_predio,
		ipr           	AS ipu,					0			    AS solar_no_edificado, 
		"bom"           AS bomberos,			'2024-01-01'::timestamp without time zone    AS fecha_emision, 
		total1         	AS total,				null   	        AS estado_liquidacion, 
		ser   			AS tasa_administrativa,	seg   			AS tasa_seguro,
		tman   			AS tasa_mantenimiento,	avaluon       	AS base_imponible, 
		'admin'	        AS usuario,				cedula       	AS ci, 
		ubi  		    AS direccion_cont,		propie          AS nombre_comprador, 
		propie       	AS propietario,			'Rural'     	AS tipo,
	    pr.anio 			AS anio,				cp.id	 		AS predio,
		"Tipo_exonera"	AS exoneracion,			'PR'			AS tipo_predio 
		FROM migrar_temp.predio_rural pr
		LEFT OUTER JOIN catastro.cat_predio cp on pr.numpre = cp.clave_cat::integer 
		LEFT JOIN sgm_financiero.ren_liquidacion L on (L.ANIO=pr.anio AND L.PREDIO = CP.ID)
		where cp.tipo_predio = 'R' and l.id is null;
	BEGIN 
		count_ = 0;
		count_= migrar_temp.insertarcatpredio_anual();
		RAISE NOTICE 'Creacion de predio nuevos %', count_;
		
		count_ = 0;
	   	FOR T IN T_TITULO_PREDIAL LOOP 
		  
		  -- continue when (select COUNT(id) from sgm_financiero.ren_liquidacion L where L.num_liquidacion=t.num_titulo AND L.ANIO=T.anio) > 0;
		 
	      nombre_compra = t.nombre_comprador;
		  num_cedula = T.ci;
	      	IF( (SELECT COUNT(*) FROM catastro.cat_ente ente WHERE ci_ruc = T.ci)>=1 ) THEN
	      			comprador = (SELECT id FROM catastro.cat_ente WHERE ci_ruc = T.ci LIMIT 1);
	      	ELSE
	      		comprador = (select cpp.ente from catastro.cat_predio_propietario cpp where cpp.predio = T.predio limit 1);
	 		END IF;
	 	IF( T.bomberos > 0 ) THEN
	 	     bomberoexist = TRUE;	
	 	ELSE
	     bomberoexist = FALSE;	
	    END IF;
	   count_ = count_ + 1;
         -- SELECT * FROM sgm_financiero.ren_estado_liquidacion;
         /*ESTADOS DE LAS LIQUIDACIONES DEL SISTEMA DEL AME*/
		IF T.estado_liquidacion is null THEN estadoLiquidacion = 2; saldo = T.total; END IF;
		IF(T.tipo = 'Rural') THEN tipoLiquidacion = 7; 
			predioUrbano = T.predio;
			IF(predioUrbano IS NULL) THEN
				predioUrbano :=  (select cp.id from catastro.cat_predio cp where cp.clave_cat = T.pre_codigo_catastral limit 1);
			END IF;	
		END IF;	
					
		IF T.fecha_emision is null then
			T.fecha_emision = now();
		end if;
	
        INSERT INTO sgm_FINANCIERO.REN_LIQUIDACION( NUM_LIQUIDACION, ID_LIQUIDACION, TIPO_LIQUIDACION, TOTAL_PAGO, USUARIO_INGRESO,
	    	FECHA_INGRESO, COMPRADOR, FECHA_CONTRATO_ANT, ESTADO_LIQUIDACION, PREDIO,ANIO, VALOR_COMERCIAL, AVALUO_MUNICIPAL, 
	    	AVALUO_CONSTRUCCION, AVALUO_SOLAR, BOMBERO, ubicacion, 
            NOMBRE_COMPRADOR, IDENTIFICACION, estado_coactiva, saldo, predio_rustico, estado_referencia, migrado, area_total,exonerado)   
      	VALUES ( T.num_liquidacion, T.num_titulo, tipoLiquidacion, T.total, 'admin', 
      		T.fecha_emision, comprador, T.fecha_emision,  estadoLiquidacion, predioUrbano, T.anio, T.val_comer_predio, (T.valor_edif_predio + T.valor_terr_predio), 
            T.valor_edif_predio, T.valor_terr_predio, bomberoexist,	T.direccion_cont, 
            nombre_compra, num_cedula, 1, saldo, predioRural, T.estado_liquidacion, true, t.area_total,exonera)
		RETURNING id INTO ID_REN_LIQUIDACION;	

         /*INGRESO DE LAS LIQUIDACIONES*/
         /*RAISE NOTICE 'Migrando Liquidaciones %',  ID_REN_LIQUIDACION;*/	
    --INSERCION DE RUBROS EN DET_LIQUIDACION CUANDO NO ESTA PAGADA SOLO EN RED_DET_LIQUIDACION
		IF(estadoLiquidacion = 2) THEN 
		--PREDIOS RURALES
			IF(T.tasa_administrativa > 0) THEN
				rubro =  23;
				INSERT INTO sgm_FINANCIERO.ren_det_liquidacion(liquidacion, rubro, valor, estado, valor_recaudado)
				    VALUES (ID_REN_LIQUIDACION, rubro, T.tasa_administrativa, TRUE, 0.00);
			END IF;	
		-- Impuesto
			IF(T.ipu > 0) THEN
				rubro = 18;		
				INSERT INTO sgm_FINANCIERO.ren_det_liquidacion(liquidacion, rubro, valor, estado, valor_recaudado)
					VALUES (ID_REN_LIQUIDACION, rubro, T.ipu, TRUE, 0.00);
			END IF;
		-- Bomberos
			IF (T.bomberos > 0) THEN
				rubro =  21;
				INSERT INTO sgm_FINANCIERO.ren_det_liquidacion( liquidacion, rubro, valor, estado, valor_recaudado)				    
				    VALUES (ID_REN_LIQUIDACION, rubro, T.bomberos, TRUE, 0.00);
			END IF;
		--tasa_seguro		
			IF(T.tasa_seguro > 0) THEN			
				rubro =  1123;				
				INSERT INTO sgm_FINANCIERO.ren_det_liquidacion(liquidacion, rubro, valor, estado,valor_recaudado)
				    VALUES (ID_REN_LIQUIDACION, rubro, T.tasa_seguro, TRUE,  0.00);
			END IF;
		--tasa_mantenimiento		
			IF(T.tasa_mantenimiento > 0) THEN			
				rubro =  1258;				
				INSERT INTO sgm_FINANCIERO.ren_det_liquidacion(liquidacion, rubro, valor, estado,valor_recaudado)
				    VALUES (ID_REN_LIQUIDACION, rubro, T.tasa_mantenimiento, TRUE,  0.00);
			END IF;
		END IF;
		IF (count_ % 100) = 0 THEN 
			RAISE NOTICE 'count_ % ', count_;
		END IF;
	END LOOP;

	RAISE NOTICE 'pROCESADOS count_ % ', count_;
END;
$function$
;

/*
-- select * from sgm_FINANCIERO.ren_tipo_liquidacion rtl 
-- select * from sgm_financiero.ren_rubros_liquidacion where tipo_liquidacion = 7
-- select * from sgm_financiero.REN_LIQUIDACION where tipo_liquidacion = 7 order by id DESC
-- select * INTO migrar_temp.predio_rura_ant from migrar_temp.predio_rural pr 
ALTER TABLE migrar_temp.predio_rural ALTER COLUMN cedula TYPE varchar USING cedula::varchar;
/*
delete from sgm_financiero.ren_det_liquidacion d using sgm_financiero.ren_liquidacion l where l.id = d.liquidacion and l.fecha_ingreso::date = '2024-01-02';
delete from sgm_financiero.ren_liquidacion l where l.fecha_ingreso::date = '2024-01-02';
*/

select count(*) from sgm_financiero.REN_LIQUIDACION where tipo_liquidacion = 7 and anio = 2024 
select migrar_temp.insertarcatpredio_anual()
select migrar_temp.migrarprediorural_anual();

select * from migrar_temp.predio_rural pr order by numpre;
select id, num_predio, clave_cat from catastro.cat_predio cp where cp.tipo_predio = 'R' order by clave_cat::bigint;

select migrar_temp.insertarcatpredio();

-- select migrar_temp.migrarprediorural()

select * from sgm_financiero.ren_liquidacion rl WHERE RL.anio = 2024

*/

-- **********************************************************************************************************************************
-- FIN RURAL 
-- **********************************************************************************************************************************