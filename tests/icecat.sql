create table `category_feature` (
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`mandatory` tinyint DEFAULT '0' null,
	`catid` integer,
	`searchable` integer DEFAULT '0',
	`no` integer DEFAULT '0',
	`category_feature_id` integer,
	`restricted_search_values` mediumtext null,
	`feature_id` integer,
	`use_dropdown_input` char(3) DEFAULT '' null,
	`category_feature_group_id` integer DEFAULT '0',
	constraint `pk_category_feature` primary key (`category_feature_id`),
	constraint `category_feature_unique0` unique (`feature_id`, `catid`),
	constraint `category_feature_unique1` unique (`feature_id`, `catid`),
	constraint `fk_category_feature_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_category_feature_category_feature_group` foreign key (`category_feature_group_id`) references `category_feature_group` (`category_feature_group_id`),
	constraint `fk_category_feature_feature` foreign key (`feature_id`) references `feature` (`feature_id`),
	constraint `fk_category_feature_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_category_feature_category_feature_group` foreign key (`category_feature_group_id`) references `category_feature_group` (`category_feature_group_id`),
	constraint `fk_category_feature_feature` foreign key (`feature_id`) references `feature` (`feature_id`)
) ENGINE=MyISAM;
create table `product_review` (
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`review_code` varchar(60) DEFAULT '',
	`review_group` varchar(60) DEFAULT '',
	`review_id` integer DEFAULT '0',
	`product_review_id` integer AUTO_INCREMENT,
	`url` varchar(255) DEFAULT '',
	`high_review_award_url` varchar(255) DEFAULT '',
	`value` text null,
	`langid` integer DEFAULT '0',
	`review_award_name` varchar(120) DEFAULT '',
	`postscriptum` text null,
	`score` integer DEFAULT '0',
	`value_bad` text null,
	`date_added` date,
	`value_good` text null,
	`product_id` integer,
	`logo_url` varchar(255) DEFAULT '',
	`low_review_award_url` varchar(255) DEFAULT '' not null,
	constraint `pk_product_review` primary key (`product_review_id`),
	constraint `product_review_unique0` unique (`product_id`, `review_id`, `langid`),
	constraint `product_review_unique1` unique (`product_id`, `review_id`, `langid`),
	constraint `fk_product_review_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_review_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_review_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_review_product` foreign key (`product_id`) references `product` (`product_id`)
) ENGINE=MyISAM;

		create trigger `tr_product_review0_insert` 
		before insert 
		on `product_review` for each row 
		begin
			
		if `new`.`date_added` is null then
			set `new`.`date_added`=now();
		end if;
		
		end; 
		;
create table `product_family` (
	`thumb_pic` varchar(255) null,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`supplier_id` integer,
	`catid` integer null,
	`symbol` varchar(120) DEFAULT '',
	`low_pic` varchar(255) null,
	`family_id` integer,
	`sid` integer,
	`tid` integer,
	`parent_family_id` integer null,
	`data_source_id` integer DEFAULT '0',
	constraint `pk_product_family` primary key (`family_id`),
	constraint `fk_product_family_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_family_parent` foreign key (`parent_family_id`) references `product_family` (`family_id`),
	constraint `fk_product_family_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_product_family_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`),
	constraint `fk_product_family_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_product_family_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_family_parent` foreign key (`parent_family_id`) references `product_family` (`family_id`),
	constraint `fk_product_family_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_product_family_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`),
	constraint `fk_product_family_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;
create table `feature_logo_category` (
	`feature_logo_id` integer,
	`category_id` integer,
	constraint `pk_feature_logo_category` primary key (`feature_logo_id`, `category_id`),
	constraint `fk_feature_logo_category_feature_logo` foreign key (`feature_logo_id`) references `feature_logo` (`feature_logo_id`),
	constraint `fk_feature_logo_category_category` foreign key (`category_id`) references `category` (`catid`),
	constraint `fk_feature_logo_category_feature_logo` foreign key (`feature_logo_id`) references `feature_logo` (`feature_logo_id`),
	constraint `fk_feature_logo_category_category` foreign key (`category_id`) references `category` (`catid`)
) ENGINE=MyISAM;
create table `feature` (
	`measure_id` integer null,
	`last_published` integer DEFAULT '0' null,
	`searchable` tinyint DEFAULT '0',
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`feature_id` integer,
	`tid` integer,
	`sid` integer,
	`limit_direction` tinyint DEFAULT '0',
	`restricted_values` mediumtext null,
	`type` varchar(60),
	`class` tinyint DEFAULT '0',
	constraint `pk_feature` primary key (`feature_id`),
	constraint `fk_feature_measure` foreign key (`measure_id`) references `measure` (`measure_id`),
	constraint `fk_feature_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_feature_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_feature_measure` foreign key (`measure_id`) references `measure` (`measure_id`),
	constraint `fk_feature_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_feature_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;
create table `measure_sign` (
	`measure_id` integer,
	`last_published` datetime,
	`measure_sign_id` integer,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`value` varchar(255),
	`langid` integer,
	constraint `pk_measure_sign` primary key (`measure_sign_id`),
	constraint `measure_sign_unique0` unique (`measure_id`, `langid`),
	constraint `measure_sign_unique1` unique (`measure_id`, `langid`),
	constraint `fk_measure_sign_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_measure_sign_measure` foreign key (`measure_id`) references `measure` (`measure_id`),
	constraint `fk_measure_sign_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_measure_sign_measure` foreign key (`measure_id`) references `measure` (`measure_id`)
) ENGINE=MyISAM;

		create trigger `tr_measure_sign0_insert` 
		before insert 
		on `measure_sign` for each row 
		begin
			
		if `new`.`last_published` is null then
			set `new`.`last_published`=now();
		end if;
		
		end; 
		;
create table `tex` (
	`tid` integer,
	`tex_id` integer,
	`langid` integer DEFAULT '0',
	`value` mediumtext,
	constraint `pk_tex` primary key (`tex_id`),
	constraint `tex_unique0` unique (`tid`, `langid`),
	constraint `tex_unique1` unique (`tid`, `langid`),
	constraint `fk_tex_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_tex_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_tex_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_tex_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;
create table `supplier` (
	`thumb_pic` varchar(255) null,
	`has_vendor_index` tinyint DEFAULT '0',
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`supplier_id` integer,
	`user_id` integer DEFAULT '1',
	`name` varchar(255) DEFAULT '',
	`acknowledge` char(1) DEFAULT 'N',
	`public_login` varchar(80) DEFAULT '' null,
	`hide_products` tinyint DEFAULT '0',
	`suppress_offers` char(1) DEFAULT 'N',
	`low_pic` varchar(255) null,
	`public_password` varchar(80) DEFAULT '' null,
	`last_name` varchar(255) DEFAULT '',
	`folder_name` varchar(255) DEFAULT '',
	`template` mediumtext null,
	`is_sponsor` char(1) DEFAULT 'N',
	`prod_id_regexp` text null,
	`ftp_homedir` varchar(255) null,
	`last_published` integer DEFAULT '0' null,
	constraint `pk_supplier` primary key (`supplier_id`),
	constraint `name` unique (`name`),
	constraint `name` unique (`name`)
) ENGINE=MyISAM;
create table `feature_logo` (
	`thumb_link` varchar(255) DEFAULT '',
	`feature_logo_id` integer AUTO_INCREMENT,
	`height` integer,
	`feature_id` integer,
	`width` integer,
	`link` varchar(255),
	`thumb_size` integer DEFAULT '0',
	`values` varchar(255) DEFAULT '',
	`size` integer,
	constraint `pk_feature_logo` primary key (`feature_logo_id`),
	constraint `feature_logo_unique0` unique (`link`),
	constraint `feature_logo_unique1` unique (`link`),
	constraint `fk_feature_logo_feature` foreign key (`feature_id`) references `feature` (`feature_id`),
	constraint `fk_feature_logo_feature` foreign key (`feature_id`) references `feature` (`feature_id`)
) ENGINE=MyISAM;
create table `tid_index` (
	`tid` integer AUTO_INCREMENT,
	`dummy` smallint null,
	constraint `pk_tid_index` primary key (`tid`)
) ENGINE=MyISAM;
create table `product_multimedia_object` (
	`keep_as_url` integer,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`uuid` varchar(40),
	`width` integer,
	`type` varchar(255),
	`is_rich` boolean,
	`preview_url` varchar(255),
	`preview_height` integer,
	`langid` integer,
	`height` integer,
	`source` varchar(255),
	`product_id` integer,
	`link` varchar(255),
	`size` integer,
	`content_type` varchar(255),
	`preview_width` integer,
	`thumb_url` varchar(255),
	`preview_size` integer,
	`id` integer AUTO_INCREMENT,
	`description` mediumtext,
	constraint `pk_product_multimedia_object` primary key (`id`),
	constraint `product_multimedia_object_unique0` unique (`product_id`, `langid`, `link`),
	constraint `product_multimedia_object_unique1` unique (`product_id`, `langid`, `link`),
	constraint `fk_product_multimedia_object_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_multimedia_object_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_multimedia_object_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_multimedia_object_language` foreign key (`langid`) references `language` (`langid`)
) ENGINE=MyISAM;
create table `product` (
	`topseller` varchar(255) DEFAULT '',
	`prod_id` varchar(60) DEFAULT '',
	`low_pic` varchar(255) DEFAULT '',
	`got_images` boolean DEFAULT 1,
	`medium_pic` varchar(255) DEFAULT '',
	`gtin` varchar(14) null,
	`quality` varchar(16),
	`supplier_id` integer,
	`user_id` integer DEFAULT '1',
	`catid` integer DEFAULT '0',
	`high_pic_size` integer DEFAULT '0' null,
	`publish` char(1) DEFAULT 'Y',
	`thumb_pic_size` integer DEFAULT '0' null,
	`launch_date` datetime null,
	`public` char(1) DEFAULT 'Y',
	`thumb_pic` varchar(255) null,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`medium_pic_width` integer DEFAULT '0',
	`medium_pic_height` integer DEFAULT '0',
	`low_pic_size` integer DEFAULT '0' null,
	`high_pic` varchar(255) DEFAULT '',
	`high_pic_origin_size` integer DEFAULT '0',
	`series_id` integer DEFAULT '1',
	`low_pic_width` integer DEFAULT '0',
	`low_pic_height` integer DEFAULT '0',
	`date_added` date,
	`product_id` integer,
	`high_pic_origin` varchar(255) DEFAULT '',
	`medium_pic_size` integer DEFAULT '0',
	`name` varchar(255) DEFAULT '',
	`on_market` boolean DEFAULT 1,
	`dname` varchar(255) DEFAULT '',
	`high_pic_width` integer DEFAULT '0',
	`checked_by_supereditor` tinyint DEFAULT '0',
	`family_id` integer DEFAULT '0',
	`high_pic_height` integer DEFAULT '0',
	`obsolence_date` integer null,
	constraint `pk_product` primary key (`product_id`),
	constraint `product_unique0` unique (`prod_id`, `supplier_id`),
	constraint `product_unique1` unique (`prod_id`, `supplier_id`),
	constraint `fk_product_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`),
	constraint `fk_product_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`)
) ENGINE=MyISAM;

		create trigger `tr_product0_insert` 
		before insert 
		on `product` for each row 
		begin
			
		if `new`.`date_added` is null then
			set `new`.`date_added`=now();
		end if;
		
		end; 
		;
create table `feature_group` (
	`feature_group_id` integer,
	`sid` integer,
	constraint `pk_feature_group` primary key (`feature_group_id`),
	constraint `fk_feature_group_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_feature_group_sidindex` foreign key (`sid`) references `sid_index` (`sid`)
) ENGINE=MyISAM;
create table `category_keywords` (
	`keywords` mediumtext null,
	`category_id` integer null,
	`langid` integer DEFAULT '0',
	`id` integer,
	constraint `pk_category_keywords` primary key (`id`),
	constraint `category_keywords_unique0` unique (`langid`, `category_id`),
	constraint `category_keywords_unique1` unique (`langid`, `category_id`),
	constraint `fk_category_keywords_category` foreign key (`category_id`) references `category` (`catid`),
	constraint `fk_category_keywords_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_category_keywords_category` foreign key (`category_id`) references `category` (`catid`),
	constraint `fk_category_keywords_language` foreign key (`langid`) references `language` (`langid`)
) ENGINE=MyISAM;
create table `product_related` (
	`preferred_option` tinyint DEFAULT '0',
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`product_id` integer,
	`rel_product_id` integer,
	`product_related_id` integer AUTO_INCREMENT,
	`order` smallint DEFAULT '0',
	`data_source_id` integer DEFAULT '0',
	constraint `pk_product_related` primary key (`product_related_id`),
	constraint `product_related_unique0` unique (`product_id`, `rel_product_id`),
	constraint `product_related_unique1` unique (`product_id`, `rel_product_id`),
	constraint `fk_product_related_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_related_related_product` foreign key (`rel_product_id`) references `product` (`product_id`),
	constraint `fk_product_related_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_related_related_product` foreign key (`rel_product_id`) references `product` (`product_id`)
) ENGINE=MyISAM;
create table `product_feature_local` (
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`product_id` integer DEFAULT '0',
	`product_feature_local_id` integer AUTO_INCREMENT,
	`category_feature_id` integer DEFAULT '0',
	`langid` integer DEFAULT '0',
	`value` varchar(15000) DEFAULT '',
	constraint `pk_product_feature_local` primary key (`product_feature_local_id`),
	constraint `product_feature_local_unique0` unique (`category_feature_id`, `product_id`, `langid`),
	constraint `product_feature_local_unique1` unique (`category_feature_id`, `product_id`, `langid`),
	constraint `fk_product_feature_local_category_feature` foreign key (`category_feature_id`) references `category_feature` (`category_feature_id`),
	constraint `fk_product_feature_local_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_feature_local_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_feature_local_category_feature` foreign key (`category_feature_id`) references `category_feature` (`category_feature_id`),
	constraint `fk_product_feature_local_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_feature_local_product` foreign key (`product_id`) references `product` (`product_id`)
) ENGINE=MyISAM;
create table `product_gallery` (
	`thumb_link` varchar(255),
	`height` integer,
	`got_images` boolean DEFAULT 1,
	`logo` boolean,
	`id` integer AUTO_INCREMENT,
	`size` integer,
	`medium_size` integer,
	`no` integer,
	`low_link` varchar(255),
	`low_height` integer,
	`size_origin` integer,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`low_size` integer,
	`link` varchar(255),
	`low_width` integer,
	`product_id` integer,
	`medium_width` integer,
	`medium_link` varchar(255),
	`langid` integer,
	`medium_height` integer,
	`source` varchar(255),
	`thumb_size` integer,
	`width` integer,
	`is_main` boolean,
	constraint `pk_product_gallery` primary key (`id`),
	constraint `product_gallery_unique0` unique (`product_id`, `link`),
	constraint `product_gallery_unique1` unique (`product_id`, `link`),
	constraint `fk_product_gallery_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_gallery_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_gallery_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_gallery_product` foreign key (`product_id`) references `product` (`product_id`)
) ENGINE=MyISAM;
create table `measure` (
	`measure_id` integer,
	`last_published` datetime,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`sign` varchar(255) null,
	`sid` integer,
	`tid` integer,
	`system_of_measurement` enum('metric','imperial') DEFAULT 'metric',
	constraint `pk_measure` primary key (`measure_id`),
	constraint `fk_measure_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_measure_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_measure_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_measure_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;

		create trigger `tr_measure0_insert` 
		before insert 
		on `measure` for each row 
		begin
			
		if `new`.`last_published` is null then
			set `new`.`last_published`=now();
		end if;
		
		end; 
		;
create table `product_feature` (
	`product_feature_id` integer AUTO_INCREMENT,
	`category_feature_id` integer,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`value` varchar(20000) DEFAULT '',
	`product_id` integer,
	constraint `pk_product_feature` primary key (`product_feature_id`),
	constraint `product_feature_unique0` unique (`category_feature_id`, `product_id`),
	constraint `product_feature_unique1` unique (`category_feature_id`, `product_id`),
	constraint `fk_product_feature_category_feature` foreign key (`category_feature_id`) references `category_feature` (`category_feature_id`),
	constraint `fk_product_feature_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_feature_category_feature` foreign key (`category_feature_id`) references `category_feature` (`category_feature_id`),
	constraint `fk_product_feature_product` foreign key (`product_id`) references `product` (`product_id`)
) ENGINE=MyISAM;
create table `language` (
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`code` varchar(32),
	`short_code` varchar(5),
	`sid` integer,
	`langid` integer,
	`published` boolean DEFAULT 1,
	`backup_langid` integer null,
	constraint `pk_language` primary key (`langid`),
	constraint `language_unique0` unique (`code`),
	constraint `language_unique1` unique (`short_code`),
	constraint `language_unique2` unique (`code`),
	constraint `language_unique3` unique (`short_code`),
	constraint `fk_language_backup_language` foreign key (`backup_langid`) references `language` (`langid`),
	constraint `fk_language_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_language_backup_language` foreign key (`backup_langid`) references `language` (`langid`),
	constraint `fk_language_sidindex` foreign key (`sid`) references `sid_index` (`sid`)
) ENGINE=MyISAM;
create table `category_feature_group` (
	`category_feature_group_id` integer,
	`feature_group_id` integer,
	`catid` integer,
	`no` integer null,
	constraint `pk_category_feature_group` primary key (`category_feature_group_id`),
	constraint `category_feature_group_unique0` unique (`catid`, `feature_group_id`),
	constraint `category_feature_group_unique1` unique (`catid`, `feature_group_id`),
	constraint `fk_category_feature_group_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_category_feature_group_feature_group` foreign key (`feature_group_id`) references `feature_group` (`feature_group_id`),
	constraint `fk_category_feature_group_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_category_feature_group_feature_group` foreign key (`feature_group_id`) references `feature_group` (`feature_group_id`)
) ENGINE=MyISAM;
create table `sid_index` (
	`dummy` smallint null,
	`sid` integer AUTO_INCREMENT,
	constraint `pk_sid_index` primary key (`sid`)
) ENGINE=MyISAM;
create table `product_series` (
	`supplier_id` integer,
	`catid` integer null,
	`series_id` integer,
	`family_id` integer,
	`sid` integer,
	`tid` integer,
	constraint `pk_product_series` primary key (`series_id`),
	constraint `fk_product_series_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_series_product_family` foreign key (`family_id`) references `product_family` (`family_id`),
	constraint `fk_product_series_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_product_series_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`),
	constraint `fk_product_series_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_product_series_category` foreign key (`catid`) references `category` (`catid`),
	constraint `fk_product_series_product_family` foreign key (`family_id`) references `product_family` (`family_id`),
	constraint `fk_product_series_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_product_series_supplier` foreign key (`supplier_id`) references `supplier` (`supplier_id`),
	constraint `fk_product_series_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;
create table `category` (
	`thumb_pic` varchar(255) DEFAULT '' null,
	`watched_top10` integer DEFAULT '0',
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`catid` integer,
	`searchable` integer DEFAULT '0',
	`ssid` integer null,
	`low_pic` varchar(255) DEFAULT '',
	`visible` integer DEFAULT '0',
	`pcatid` integer null,
	`sid` integer,
	`tid` integer,
	`ucatid` varchar(255) null,
	`last_published` integer DEFAULT '0' null,
	constraint `pk_category` primary key (`catid`),
	constraint `ucatid` unique (`ucatid`),
	constraint `ucatid` unique (`ucatid`),
	constraint `fk_category_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_category_tidindex` foreign key (`tid`) references `tid_index` (`tid`),
	constraint `fk_category_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_category_tidindex` foreign key (`tid`) references `tid_index` (`tid`)
) ENGINE=MyISAM;
create table `vocabulary` (
	`record_id` integer,
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`langid` integer DEFAULT '0',
	`value` varchar(255) null,
	`sid` integer,
	constraint `pk_vocabulary` primary key (`record_id`),
	constraint `vocabulary_unique0` unique (`sid`, `langid`),
	constraint `vocabulary_unique1` unique (`sid`, `langid`),
	constraint `fk_vocabulary_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_vocabulary_sidindex` foreign key (`sid`) references `sid_index` (`sid`),
	constraint `fk_vocabulary_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_vocabulary_sidindex` foreign key (`sid`) references `sid_index` (`sid`)
) ENGINE=MyISAM;
create table `product_description` (
	`product_description_id` integer AUTO_INCREMENT,
	`manual_pdf_url` varchar(255) DEFAULT '',
	`updated` timestamp DEFAULT now() ON UPDATE now(),
	`specs_url` varchar(512) DEFAULT '',
	`option_field_1` mediumtext null,
	`product_id` integer,
	`option_field_2` mediumtext null,
	`support_url` varchar(255) DEFAULT '',
	`pdf_url` varchar(255) DEFAULT '',
	`short_desc` varchar(3000) DEFAULT '',
	`langid` integer,
	`pdf_size` integer DEFAULT '0' null,
	`manual_pdf_size` integer DEFAULT '0' null,
	`long_desc` mediumtext,
	`manual_pdf_updated` integer DEFAULT '0',
	`manual_pdf_url_origin` text null,
	`warranty_info` mediumtext null,
	`pdf_url_origin` text null,
	`pdf_updated` integer DEFAULT '0',
	`official_url` text null,
	constraint `pk_product_description` primary key (`product_description_id`),
	constraint `product_description_unique0` unique (`product_id`, `langid`),
	constraint `product_description_unique1` unique (`product_id`, `langid`),
	constraint `fk_product_description_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_description_language` foreign key (`langid`) references `language` (`langid`),
	constraint `fk_product_description_product` foreign key (`product_id`) references `product` (`product_id`),
	constraint `fk_product_description_language` foreign key (`langid`) references `language` (`langid`)
) ENGINE=MyISAM;