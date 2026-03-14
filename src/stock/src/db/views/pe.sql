-- === VIEW: v_pe ===
CREATE OR REPLACE VIEW public.v_pe AS
SELECT
    mv_pe."Ticker",
    mv_pe."Trailing PE",
    mv_pe."Forward PE",
    mv_pe.last_update,
    mv_pe.date,
    mv_pe.rn
FROM public.mv_pe;
