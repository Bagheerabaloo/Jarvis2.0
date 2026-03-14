-- === VIEW: v_last_info_sector_industry ===
CREATE OR REPLACE VIEW public.v_last_info_sector_industry AS
SELECT
    mv_last_info_sector_industry.ticker_id,
    mv_last_info_sector_industry.sector,
    mv_last_info_sector_industry.industry,
    mv_last_info_sector_industry.last_update
FROM public.mv_last_info_sector_industry;
