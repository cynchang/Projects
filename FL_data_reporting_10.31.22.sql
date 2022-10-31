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
    -- unioning events 1-3
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
    )
,

-- OON Referrals --
event4(event_type
       , event_date
       , event_count)
as (SELECT cast('OON Refs' as varchar) as event_type
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

    -- unioning events 1-3 with 4
total2(event_type
    , event_date
    , event_count)
    as (select cas.event_type
        ,cas.event_date
        ,cas.event_count
        from cas_ref cas
        union
        select e4.event_type
        ,e4.event_date
        ,e4.event_count
        )


select t.event_type
, t.event_date
, t.event_count
from total2 t;
