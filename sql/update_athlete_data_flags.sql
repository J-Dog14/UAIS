-- Function to update athlete data presence flags and session counts
-- This function should be called periodically or via triggers to keep data in sync

CREATE OR REPLACE FUNCTION update_athlete_data_flags()
RETURNS void AS $$
BEGIN
    -- Update flags and counts for all athletes
    UPDATE analytics.d_athletes a
    SET
        -- Pitching data
        has_pitching_data = EXISTS (
            SELECT 1 FROM public.f_kinematics_pitching p 
            WHERE p.athlete_uuid = a.athlete_uuid
        ),
        pitching_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_kinematics_pitching p 
            WHERE p.athlete_uuid = a.athlete_uuid
        ),
        
        -- Athletic Screen data
        has_athletic_screen_data = EXISTS (
            SELECT 1 FROM public.f_athletic_screen s 
            WHERE s.athlete_uuid = a.athlete_uuid
        ),
        athletic_screen_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_athletic_screen s 
            WHERE s.athlete_uuid = a.athlete_uuid
        ),
        
        -- Pro-Sup data
        has_pro_sup_data = EXISTS (
            SELECT 1 FROM public.f_pro_sup ps 
            WHERE ps.athlete_uuid = a.athlete_uuid
        ),
        pro_sup_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_pro_sup ps 
            WHERE ps.athlete_uuid = a.athlete_uuid
        ),
        
        -- Readiness Screen data
        has_readiness_screen_data = EXISTS (
            SELECT 1 FROM public.f_readiness_screen rs 
            WHERE rs.athlete_uuid = a.athlete_uuid
        ),
        readiness_screen_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_readiness_screen rs 
            WHERE rs.athlete_uuid = a.athlete_uuid
        ),
        
        -- Mobility data
        has_mobility_data = EXISTS (
            SELECT 1 FROM public.f_mobility m 
            WHERE m.athlete_uuid = a.athlete_uuid
        ),
        mobility_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_mobility m 
            WHERE m.athlete_uuid = a.athlete_uuid
        ),
        
        -- Proteus data
        has_proteus_data = EXISTS (
            SELECT 1 FROM public.f_proteus pr 
            WHERE pr.athlete_uuid = a.athlete_uuid
        ),
        proteus_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_proteus pr 
            WHERE pr.athlete_uuid = a.athlete_uuid
        ),
        
        -- Hitting data
        has_hitting_data = EXISTS (
            SELECT 1 FROM public.f_kinematics_hitting h 
            WHERE h.athlete_uuid = a.athlete_uuid
        ),
        hitting_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_kinematics_hitting h 
            WHERE h.athlete_uuid = a.athlete_uuid
        ),
        
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Create a trigger function to update flags when fact tables change
-- This will automatically update flags when data is inserted/updated/deleted
-- Optimized to only update the affected athlete

CREATE OR REPLACE FUNCTION trigger_update_athlete_flags()
RETURNS TRIGGER AS $$
DECLARE
    affected_uuid VARCHAR(36);
BEGIN
    -- Get the athlete UUID from the changed row
    affected_uuid := COALESCE(NEW.athlete_uuid, OLD.athlete_uuid);
    
    -- Update flags for only the affected athlete
    UPDATE analytics.d_athletes a
    SET
        -- Pitching data
        has_pitching_data = EXISTS (
            SELECT 1 FROM public.f_kinematics_pitching p 
            WHERE p.athlete_uuid = a.athlete_uuid
        ),
        pitching_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_kinematics_pitching p 
            WHERE p.athlete_uuid = a.athlete_uuid
        ),
        
        -- Athletic Screen data
        has_athletic_screen_data = EXISTS (
            SELECT 1 FROM public.f_athletic_screen s 
            WHERE s.athlete_uuid = a.athlete_uuid
        ),
        athletic_screen_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_athletic_screen s 
            WHERE s.athlete_uuid = a.athlete_uuid
        ),
        
        -- Pro-Sup data
        has_pro_sup_data = EXISTS (
            SELECT 1 FROM public.f_pro_sup ps 
            WHERE ps.athlete_uuid = a.athlete_uuid
        ),
        pro_sup_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_pro_sup ps 
            WHERE ps.athlete_uuid = a.athlete_uuid
        ),
        
        -- Readiness Screen data
        has_readiness_screen_data = EXISTS (
            SELECT 1 FROM public.f_readiness_screen rs 
            WHERE rs.athlete_uuid = a.athlete_uuid
        ),
        readiness_screen_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_readiness_screen rs 
            WHERE rs.athlete_uuid = a.athlete_uuid
        ),
        
        -- Mobility data
        has_mobility_data = EXISTS (
            SELECT 1 FROM public.f_mobility m 
            WHERE m.athlete_uuid = a.athlete_uuid
        ),
        mobility_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_mobility m 
            WHERE m.athlete_uuid = a.athlete_uuid
        ),
        
        -- Proteus data
        has_proteus_data = EXISTS (
            SELECT 1 FROM public.f_proteus pr 
            WHERE pr.athlete_uuid = a.athlete_uuid
        ),
        proteus_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_proteus pr 
            WHERE pr.athlete_uuid = a.athlete_uuid
        ),
        
        -- Hitting data
        has_hitting_data = EXISTS (
            SELECT 1 FROM public.f_kinematics_hitting h 
            WHERE h.athlete_uuid = a.athlete_uuid
        ),
        hitting_session_count = (
            SELECT COUNT(DISTINCT session_date) 
            FROM public.f_kinematics_hitting h 
            WHERE h.athlete_uuid = a.athlete_uuid
        ),
        
        updated_at = NOW()
    WHERE a.athlete_uuid = affected_uuid;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create triggers on each fact table to auto-update flags
-- These triggers automatically update flags for the affected athlete when data changes

-- Pitching
DROP TRIGGER IF EXISTS trg_update_flags_pitching ON public.f_kinematics_pitching;
CREATE TRIGGER trg_update_flags_pitching
    AFTER INSERT OR UPDATE OR DELETE ON public.f_kinematics_pitching
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Athletic Screen
DROP TRIGGER IF EXISTS trg_update_flags_athletic_screen ON public.f_athletic_screen;
CREATE TRIGGER trg_update_flags_athletic_screen
    AFTER INSERT OR UPDATE OR DELETE ON public.f_athletic_screen
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Pro-Sup
DROP TRIGGER IF EXISTS trg_update_flags_pro_sup ON public.f_pro_sup;
CREATE TRIGGER trg_update_flags_pro_sup
    AFTER INSERT OR UPDATE OR DELETE ON public.f_pro_sup
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Readiness Screen
DROP TRIGGER IF EXISTS trg_update_flags_readiness_screen ON public.f_readiness_screen;
CREATE TRIGGER trg_update_flags_readiness_screen
    AFTER INSERT OR UPDATE OR DELETE ON public.f_readiness_screen
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Mobility
DROP TRIGGER IF EXISTS trg_update_flags_mobility ON public.f_mobility;
CREATE TRIGGER trg_update_flags_mobility
    AFTER INSERT OR UPDATE OR DELETE ON public.f_mobility
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Proteus
DROP TRIGGER IF EXISTS trg_update_flags_proteus ON public.f_proteus;
CREATE TRIGGER trg_update_flags_proteus
    AFTER INSERT OR UPDATE OR DELETE ON public.f_proteus
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Hitting
DROP TRIGGER IF EXISTS trg_update_flags_hitting ON public.f_kinematics_hitting;
CREATE TRIGGER trg_update_flags_hitting
    AFTER INSERT OR UPDATE OR DELETE ON public.f_kinematics_hitting
    FOR EACH ROW
    EXECUTE FUNCTION trigger_update_athlete_flags();

-- Manual update function (call this to refresh all flags)
-- Usage: SELECT update_athlete_data_flags();

