#Take the Center for Responsive Politics indivs table and create a list of donors to include in our similarities




# donor-recip totals
drop table if exists gensim_1_contrib_recip;
create table gensim_1_contrib_recip select contribidshort, recipid, SUM(AMOUNT) FROM crp_indivs where cycle=2012 GROUP BY contribidshort, recipid;

#2: donors who have given to at least 6 people
drop table if exists gensim_2_contrib_recip;
create table gensim_2_contrib_recip select a.* from gensim_1_contrib_recip a, (select contribidshort from gensim_1_contrib_recip group by contribidshort having count(recipid)>5) b where a.contribidshort=b.contribidshort and a.contribidshort<>"" and a.recipid<>""; 
alter table gensim_2_contrib_recip add index(contribidshort);

#3: get info for donors, populating it with sample descriptors that may not be the best
drop table if exists gensim_3_contrib;
create table gensim_3_contrib select contribidshort, max(lastname) as lastname, max(first) as first, max(orgname) as orgname, max(realcode) as realcode, max(city) as city, max(state) as state from crp_indivs WHERE cycle=2012 and contribidshort in (select distinct contribidshort from gensim_2_contrib_recip) group by contribidshort;
alter table gensim_3_contrib add primary key(contribidshort);


#3a: update with most common names
UPDATE gensim_3_contrib INNER JOIN (
select a.* from (
        select crp_indivs.contribidshort, crp_indivs.lastname, crp_indivs.first, count(*) as n
        from crp_indivs INNER JOIN gensim_3_contrib ON crp_indivs.contribidshort=gensim_3_contrib.contribidshort WHERE cycle=2012 group by crp_indivs.contribidshort, lastname, first
    ) as a inner join (
        select contribidshort, max(n) as maxn from (
            select crp_indivs.contribidshort, crp_indivs.lastname, crp_indivs.first, count(*) as n from crp_indivs INNER JOIN gensim_3_contrib ON crp_indivs.contribidshort=gensim_3_contrib.contribidshort WHERE cycle=2012 group by crp_indivs.contribidshort, lastname, first
        ) aa group by contribidshort   
) as b on a.contribidshort = b.contribidshort and a.n=b.maxn
) whole ON (whole.contribidshort=gensim_3_contrib.contribidshort)
SET gensim_3_contrib.lastname=whole.lastname, gensim_3_contrib.first=whole.first;



#3b: update with most common orgs + realcodes, not counting special ones
UPDATE gensim_3_contrib INNER JOIN (
select a.* from (
        select crp_indivs.contribidshort, crp_indivs.orgname, crp_indivs.realcode, count(*) as n
        from crp_indivs INNER JOIN gensim_3_contrib ON crp_indivs.contribidshort=gensim_3_contrib.contribidshort 
        WHERE cycle=2012 AND crp_indivs.realcode not like'Z%' and crp_indivs.realcode not like 'J%' 
        group by crp_indivs.contribidshort, orgname, realcode
    ) as a inner join (
        select contribidshort, max(n) as maxn from (
            select crp_indivs.contribidshort, crp_indivs.orgname, crp_indivs.realcode, count(*) as n from crp_indivs INNER JOIN gensim_3_contrib ON crp_indivs.contribidshort=gensim_3_contrib.contribidshort WHERE cycle=2012 AND crp_indivs.realcode not like 'Z%' and crp_indivs.realcode not like 'J%' 
            group by crp_indivs.contribidshort, orgname, realcode
        ) aa group by contribidshort   
) as b on a.contribidshort = b.contribidshort and a.n=b.maxn
) whole ON (whole.contribidshort=gensim_3_contrib.contribidshort)
SET gensim_3_contrib.orgname=whole.orgname, gensim_3_contrib.realcode=whole.realcode;



