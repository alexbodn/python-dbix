use utf8;

package Icecat::Schema::Result::ProductReview;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("product_review");
__PACKAGE__->add_columns(
    product_review_id => { data_type => "integer", is_auto_increment => 1 },
    product_id        => { data_type => "integer" },
    langid            => { data_type => "integer", default_value     => 0 },
    review_group      => {
        data_type     => "varchar",
        default_value => "",
        size          => 60
    },
    review_code => {
        data_type     => "varchar",
        default_value => "",
        size          => 60
    },
    review_id => { data_type => "integer", default_value => 0 },
    score     => { data_type => "integer", default_value => 0 },
    url       => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    logo_url => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    value             => { data_type => "text", is_nullable => 1 },
    value_good        => { data_type => "text", is_nullable => 1 },
    value_bad         => { data_type => "text", is_nullable => 1 },
    postscriptum      => { data_type => "text", is_nullable => 1 },
    review_award_name => {
        data_type     => "varchar",
        default_value => "",
        size          => 120
    },
    high_review_award_url => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    low_review_award_url => {
        data_type     => "varchar",
        default_value => "",
        is_nullable   => 0,
        size          => 255
    },
    date_added => {
        data_type     => "date",
        set_on_create => 1
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("product_review_id");
__PACKAGE__->add_unique_constraint( [ "product_id", "review_id", "langid" ] );

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

1;
