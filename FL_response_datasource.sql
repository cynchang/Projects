----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
---FLORIDA EMERGENCY RESPONSE--
----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
/*
-Number of Programs Available (Out of 1,500)
-Number of Constituents with a Successful Transaction (Out of 150,000)
-Number of Successful Transactions (Out of 750,000)
    For each transaction:
        - # of AR forms submitted
            /* NOTE: From Molly B:
                we aren't counting clients there because with Platform Upgrade,
                AR submissions creates a client record, which leads to duplicates.
                Our assumption is that any client that has an AR submitted will have an assessment completed or a referral or
                out-of-network case created, or a phone call made to them by NHS. So we're already capturing them with the other metrics
                in the clients count. */

        - # of assessments submitted
        - # of referrals sent
            /* NOTE: From Molly B:
                - I think let's exclude auto-recalled for now (though we are considering updating our metrics to include them in the future,
                    we haven't finalized that anywhere yet). We want to be able to capture all referral attempts made, whether they are in-network or out-of-network,
                    so we do need to include referrals in the off_platform status. I do understand, however, wanting to keep to our standard definitions.
                    I'm wondering if we just separate in-network referrals - where we exclude referrals in draft, auto-recalled, and off-platform -
                    and out-of-network referrals, where we report on referrals in the off_platform status, as well as the cases in the off_platform status with no referrals associated.
                - referral count and oon counts will be added together for 'referrals sent' metric
                - in-network referrals: referrals not in draft, auto-recalled, or off-platform status
                - out-of-network referrals: referrals in off-platform status + cases in off_platform status that do not have an associated referral in off-platform status */


        - # of non-automated phone consultations
             /* NOTE:
                - we are counting submission_ids that have the term %call% in the response_value - this was an approach approved by Florida reporting team                                                                                                                       */
                - assuming that all submissions are connected to a case */


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

--1) Number of Programs Available.
--        LOGIC: All programs that are tied to Florida or in the Unite US East Network, but state of Florida.

select count (distinct entity_id), cast(entity_created_at as date) as date
from da_reporting_layer.rl_network_coverage
where network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802'
and entity_state = 'FL'
and entity_type = 'Program'
group by 2
order by 2;

----------------------------------------------------------------------------------------------------------------
--2) Successful Transaction and Client Counts
-- AR FORMS SUBMITTED --
--# transactions
with
    --ARs--
    event1 (event_type
        , event_date
       , event_count)
as (select cast('AR' as varchar) as event_type
         , CAST(cas.created_at AS DATE) as event_date
        ,count (distinct cas.case_id) as event_count
from da_data_marts.da_cases_snapshot cas
left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
    on cas.originating_provider_id = org_addr.provider_id
left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
    on cas.person_id = cli_addr.client_id
where cas.assistance_request = TRUE
-- and cas.originating_network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802' --Unite Us East Network --removing because ARs aren't assigned a network until they are processed
and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')
 and (cas.originating_provider_id in ( 'e37fe4a2-b5d9-4283-ac7f-81de64748ad1')) --Hurricane Ian Response - Premium Network Hub Support
group by CAST(cas.created_at AS DATE)
order by CAST(cas.created_at AS DATE))

,
-- ASSESSMENTS SUBMITTED --
event2 (event_type
       , event_date
       , event_count)

as (select cast('Assessments' as varchar) as event_type
            ,cast(date as date) as event_date
            ,count (distinct assess.transactions) as event_count
    from (select distinct cas.person_id as clients
     ,  sub.submission_id as transactions
     , sub.created_at  as date
from da_data_marts.da_form_submissions_snapshot sub
left join  da_data_marts.da_submission_responses_snapshot sub_resp
    on sub.submission_id = sub_resp.submission_id
inner join da_data_marts.da_forms_snapshot f
    on sub.form_id = f.form_id
left join da_data_marts.da_network_providers_snapshot netprov
    on sub.provider_id = netprov.provider_id
inner join da_data_marts.da_cases_snapshot cas
        on sub.context_id = cas.case_id
        and sub.context_type = 'Case'
left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
    on cas.originating_provider_id = org_addr.provider_id
left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
    on cas.person_id = cli_addr.client_id
where sub.created_at >= '10-08-2022'
and netprov.network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802'
and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')
and f.form_id in ('8046764f-e5a3-4f02-ae92-cc74d1de7340', 'b6bd66d9-41f4-4fdb-aae7-36aacc2452a6', '3d12b467-aa2f-475c-b8fa-c8dda1e6ac5f'
                 ,'ae11c6e4-58ea-4ac9-b5e3-f31a60b088ba', '03b0f52d-b03c-4480-a67e-0bbe7436e1e3', '0fe0a60e-d5e5-4255-aebb-fb2d1a8a3d40'
                 ,'49a4ca58-5877-4a4d-8175-b3d7057071e0', 'b3096c52-fe7c-4f44-a543-64806019e8e9', '57a5f20c-938e-4375-b2fa-6f923195d8e8'
                 ,'7d10d601-eb70-4660-8f79-7b921960fd34', 'd2e50af6-3a91-48cf-972f-003efca30555', '7e3b281e-c17e-4bf9-8cc7-c27f5700b897'
                 , 'af566dc2-9f7d-482e-b745-130fafd6897f', '54602bd1-9f8d-4270-9a66-dc93af431ca6', 'f711978d-4159-4e31-8054-6a7fbdd542d3'
                 , '4aa749da-30d2-4cb6-9929-e399969811bd', 'fcdfb45b-f14a-4f67-be50-c8748e2e6bb3', 'a72d2697-747c-4823-8e8c-72e3982f4607'
                 , '0244c8a8-35b6-46f3-836b-f85231fc554a', 'ac326bbf-29a4-4ede-8bab-8b054c3d10ca', 'c84900ae-93e9-4626-80f6-35c7d1d00a5d'
                 , 'cef8eaa7-a379-4344-90ec-1f91f40a9147', 'cfc56080-a588-49f9-be66-352e7788261c', '7e21bf92-1178-4fcf-9d64-d45fb6f92ba2'
                 , '263ad53c-3e34-45e7-8b58-ea9e98c43714', '488e01bc-00af-4491-9980-6c2d73b6aeca')
and f.form_name not ilike '%Assistance Request%') as assess
group by cast(date as date)
order by cast(date as date))
,
-- REFERRALS --
event3(event_type
       , event_date
       , event_count)
as (SELECT cast('Referrals' as varchar) as event_type
     , cast(date as date) as event_date
         , count(distinct refer.transactions) as event_count
     from(select distinct cas.person_id as clients
     , ref.referral_id as transactions
     , ref.created_at as date
FROM  da_reporting_layer.rl_referrals_snapshot ref
	INNER JOIN  da_data_marts.da_cases_snapshot  cas
		ON cas.case_id = ref.case_id
	INNER JOIN  da_data_marts.da_providers_snapshot  prov
		ON ref.sending_provider_id = prov.provider_id
    left join da_data_marts.da_providers_snapshot  rec_prov
        on ref.receiving_provider_id  = rec_prov.provider_id
left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
    on ref.sending_provider_id = org_addr.provider_id
left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
    on cas.person_id = cli_addr.client_id
WHERE ref.state not in ('draft', 'auto_recalled', 'off_platform')
    and ref.sending_network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802'
    and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')
    and ref.created_at >= '10-08-2022') as refer
group by cast(date as date)
order by cast(date as date))
,


-- OON Referrals --
event4(event_type
       , event_date
       , event_count)
as ( SELECT cast('OON Refs' as varchar) as event_type
             , cast(date as date) as event_date
               , count(distinct oonr.transactions) as event_count
          from (SELECT distinct cas.person_id   as clients
                              , ref.referral_id as transactions
                              , ref.created_at  as date
                FROM da_reporting_layer.rl_referrals_snapshot ref
                         INNER JOIN da_data_marts.da_cases_snapshot cas
                                    ON cas.case_id = ref.case_id
                         INNER JOIN da_data_marts.da_providers_snapshot prov
                                    ON ref.sending_provider_id = prov.provider_id
                         left join da_data_marts.da_providers_snapshot rec_prov
                                   on ref.receiving_provider_id = rec_prov.provider_id
                         left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
                                   on ref.sending_provider_id = org_addr.provider_id
                         left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
                                   on cas.person_id = cli_addr.client_id
                WHERE ref.state = 'off_platform'
                  and ref.sending_network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802'
                  and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')
                  and ref.created_at >= '10-08-2022') as oonr
          group by cast(date as date)
          order by cast(date as date))
,

--OON Cases--
event5(event_type
       , event_date
       , event_count)
as(SELECT cast('OON_Case' as varchar) as event_type
     , cast(date as date) as event_date
     , count(distinct oonc.transactions) as event_count
     from(select distinct cas.person_id as clients
             , cas.case_id as transactions
             , cas.processed_at as date --on 10/19 changed from created_at to processed_at because using created_at causes day-to-day count issues (approved by Molly B.)
        from da_data_marts.da_cases_snapshot cas
        left join da_reporting_layer.rl_service_events se
            on cas.case_id = se.case_id
        left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
            on cas.originating_provider_id = org_addr.provider_id
        left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
            on cas.person_id = cli_addr.client_id
        where network_id =  'ac49506d-8f86-45cb-804a-9bc604ef3802'
            and status = 'off_platform'
            and se.case_is_referred = FALSE
            and cas.created_at >= '10-08-2022'
            and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')) as oonc
group by cast(date as date)
order by cast(date as date))
,

-- NON AUTOMATED PHONE CALLS --
event6(event_type
       , event_date
       , event_count)
as (SELECT cast('Non-automated phone call' as varchar) as event_type
    , cast(date as date) as event_date
    , count(distinct phone.transactions) as event_count
    from(select distinct case when sub.context_type = 'Person' then sub.context_id end as clients
        , sub.submission_id as transactions
        , sub.created_at as date
    from da_data_marts.da_form_submissions_snapshot sub
    left join  da_data_marts.da_submission_responses_snapshot sub_resp
        on sub.submission_id = sub_resp.submission_id
    inner join da_data_marts.da_forms_snapshot f
        on sub.form_id = f.form_id
        and f.form_id = 'deecae0a-9946-4b85-a8f9-336c64a74b7d' ----NHS Interactions Assessment
    left join da_data_marts.da_network_providers_snapshot netprov
        on sub.provider_id = netprov.provider_id
    -- left join da_data_marts.da_cases_snapshot cas
    --         on sub.context_id = cas.case_id
    --         and sub.context_type = 'Case'
    -- left join da_edw_seed.mv_org_geocoded_addresses_current org_addr
    --     on cas.originating_provider_id = org_addr.provider_id
    -- left join da_edw_seed.mv_client_geocoded_addresses_current cli_addr
    --     on cas.person_id = cli_addr.client_id
    where sub.created_at >= '10-08-2022'
    -- and netprov.network_id = 'ac49506d-8f86-45cb-804a-9bc604ef3802'
    and sub.provider_id in ('e37fe4a2-b5d9-4283-ac7f-81de64748ad1', 'e56fe68f-830e-498f-a62d-4baf31a4b983') -- Hurricane Ian Response - Premium Network Hub Support & Florida Network Hub Premium Support
    and sub_resp.response_value ilike '%call%'-- this logic was confirmed by the Fl response team
    -- and (org_addr.state_2 = 'FL' or cli_addr.state_2 = 'FL')
                                    ) as phone
group by cast(date as date)
order by cast(date as date))
,
/* 10/26 Update:
   Based on conversations with Molly and Brian, we are adjusting our methodology on how we are counting non-automatic phone calls.
   We went from using the form_id and Florida geography to removing Florida geography and using the specific orgs that are involved in running the calls.
*/
    -- unioning events 1-5 --
cas_ref(event_type,
    event_date,
    event_count)
    as(
select e1.event_type
        , e1.event_date
        , e1.event_count
    from event1 e1
    union
        select
            e2.event_type
            , e2.event_date
            , e2.event_count
        from event2 e2
   union
    select
        e3.event_type
        , e3.event_date
        , e3.event_count
    from event3 e3
    union
        select e4.event_type
        ,e4.event_date
        ,e4.event_count
        from event4 e4
    union
        select e5.event_type
        ,e5.event_date
        ,e5.event_count
        from event5 e5
    union
        select e6.event_type
        ,e6.event_date
        ,e6.event_count
        from event6 e6
    )


select cas.event_type
, cas.event_date
, cas.event_count
from cas_ref cas;
