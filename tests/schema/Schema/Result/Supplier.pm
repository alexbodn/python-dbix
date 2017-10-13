use utf8;

package Icecat::Schema::Result::Supplier;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("supplier");
__PACKAGE__->add_columns(
    supplier_id => { data_type => "integer" },
    user_id     => { data_type => "integer", default_value => 1 },
    name => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    low_pic     => { data_type => "varchar", is_nullable => 1, size => 255 },
    thumb_pic   => { data_type => "varchar", is_nullable => 1, size => 255 },
    acknowledge => {
        data_type     => "char",
        default_value => "N",
        size          => 1
    },
    is_sponsor => {
        data_type     => "char",
        default_value => "N",
        size          => 1
    },
    public_login => {
        data_type     => "varchar",
        default_value => "",
        is_nullable   => 1,
        size          => 80
    },
    public_password => {
        data_type     => "varchar",
        default_value => "",
        is_nullable   => 1,
        size          => 80
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    last_published =>
      { data_type => "integer", default_value => 0, is_nullable => 1 },
    ftp_homedir => { data_type => "varchar", is_nullable => 1, size => 255 },
    template    => { data_type => "mediumtext", is_nullable => 1 },
    folder_name => {
        data_type     => "varchar",
        default_value => "",
        size => 255,
    },
    suppress_offers => {
        data_type     => "char",
        default_value => "N",
        size          => 1
    },
    last_name => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    prod_id_regexp   => { data_type => "text",    is_nullable   => 1 },
    has_vendor_index => { data_type => "tinyint", default_value => 0 },
    hide_products    => { data_type => "tinyint", default_value => 0 },
);
__PACKAGE__->set_primary_key("supplier_id");
__PACKAGE__->add_unique_constraint( "name", ["name"] );

__PACKAGE__->has_many(
    products => "Icecat::Schema::Result::Product",
    "supplier_id"
);

__PACKAGE__->has_many(
    product_families => "Icecat::Schema::Result::ProductFamily",
    "supplier_id"
);

__PACKAGE__->has_many(
    product_series => "Icecat::Schema::Result::ProductSeries",
    "supplier_id"
);

1;
