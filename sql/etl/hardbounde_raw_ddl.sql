-- Drop and recreate table
drop table if exists {{ params.raw_table }};
create table if not exists {{ params.raw_table }}
        (
            emailDomain_cat varchar(255)
            ,emailDomainPiece1 varchar(255)
            ,emailDomainPiece2 varchar(255)
            ,regDate_n varchar(255)
            ,birthDate_n varchar(255)
            ,monthsSinceRegDate varchar(255)
            ,age varchar(255)
            ,percNumbersInEmailUser varchar(255)
            ,hasNumberInEmailUser varchar(255)
            ,emailUserCharQty varchar(255)
            ,flgHardBounce_n varchar(255)
        );