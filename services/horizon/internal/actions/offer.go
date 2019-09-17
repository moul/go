package actions

import (
	"context"
	"net/http"

	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/resourceadapter"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/xdr"
)

// GetOffersHandler is the action handler for the /offers endpoint
type GetOffersHandler struct {
	HistoryQ *history.Q
}

// QueryParams query struct for pagination params
// TODO: move a shared package - maybe even db2
type QueryParams struct {
	Cursor string `schema:"cursor"`
	Order  string `schema:"order"`
	Limit  uint64 `schema:"limit"`
}

// OffersQuery query struct for offers end-point
type OffersQuery struct {
	QueryParams
	Seller              string `schema:"seller"`
	SellingAssetType    string `schema:"selling_asset_type"`
	SellingAsssetIssuer string `schema:"selling_asset_issuer"`
	SellingAsssetCode   string `schema:"selling_asset_code"`
	BuyingAssetType     string `schema:"buying_asset_type"`
	BuyingAsssetIssuer  string `schema:"buying_asset_issuer"`
	BuyingAsssetCode    string `schema:"buying_asset_code"`
}

// PageQuery returns the page query.
func (q OffersQuery) PageQuery() (db2.PageQuery, error) {
	pageQuery, err := db2.NewPageQuery(q.Cursor, true, q.Order, q.Limit)

	if err != nil {
		return pageQuery, problem.MakeInvalidFieldProblem(
			"pagination parameters",
			err,
		)
	}

	return pageQuery, nil
}

// HasSelling returns whether the query has a selling asset param or not.
func (q OffersQuery) HasSelling() bool {
	return len(q.SellingAssetType) > 0
}

// Selling an xdr.Asset representing the selling side of the offer.
func (q OffersQuery) Selling() (xdr.Asset, error) {
	selling, err := BuildAsset(q.SellingAssetType, q.SellingAsssetIssuer, q.SellingAsssetCode)

	if err != nil {
		return selling, problem.MakeInvalidFieldProblem(
			// unfortunate effect here we loss the ability to tell exactly which
			// param is wrong
			"selling asset",
			err,
		)
	}

	return selling, nil
}

// HasBuying returns whether the query has a buying asset param or not.
func (q OffersQuery) HasBuying() bool {
	return len(q.SellingAssetType) > 0
}

// Buying an xdr.Asset representing the buying side of the offer.
func (q OffersQuery) Buying() (xdr.Asset, error) {
	buying, err := BuildAsset(q.BuyingAssetType, q.BuyingAsssetIssuer, q.BuyingAsssetCode)

	if err != nil {
		return buying, problem.MakeInvalidFieldProblem(
			"buying asset",
			err,
		)
	}

	return buying, nil
}

// SellerAccountID returns an xdr.AcccountID for the given seller query param.
func (q OffersQuery) SellerAccountID() (xdr.AccountId, error) {
	return buildAccountID(q.Seller)
}

// GetResourcePage returns a page of offers.
func (handler GetOffersHandler) GetResourcePage(r *http.Request) ([]hal.Pageable, error) {
	ctx := r.Context()
	qp := OffersQuery{}
	err := GetParams(&qp, r)

	if err != nil {
		return nil, err
	}

	pq, err := qp.PageQuery()

	if err != nil {
		return nil, err
	}

	seller, err := qp.SellerAccountID()

	if err != nil {
		return nil, err
	}

	var selling *xdr.Asset
	if qp.HasSelling() {
		sellingAsset, err := qp.Selling()
		if err != nil {
			return nil, err
		}
		selling = &sellingAsset
	}

	var buying *xdr.Asset

	if qp.HasBuying() {
		buyingAsset, err := qp.Buying()
		if err != nil {
			return nil, err
		}
		buying = &buyingAsset
	}

	query := history.OffersQuery{
		PageQuery: pq,
		SellerID:  seller.Address(),
		Selling:   selling,
		Buying:    buying,
	}

	offers, err := getOffersPage(ctx, handler.HistoryQ, query)
	if err != nil {
		return nil, err
	}

	return offers, nil
}

// GetAccountOffersHandler is the action handler for the
// `/accounts/{account_id}/offers` endpoint when using experimental ingestion.
type GetAccountOffersHandler struct {
	HistoryQ *history.Q
}

func (handler GetAccountOffersHandler) parseOffersQuery(r *http.Request) (history.OffersQuery, error) {
	pq, err := GetPageQuery(r)
	if err != nil {
		return history.OffersQuery{}, err
	}

	seller, err := GetString(r, "account_id")
	if err != nil {
		return history.OffersQuery{}, err
	}

	query := history.OffersQuery{
		PageQuery: pq,
		SellerID:  seller,
	}

	return query, nil
}

// GetResourcePage returns a page of offers for a given account.
func (handler GetAccountOffersHandler) GetResourcePage(r *http.Request) ([]hal.Pageable, error) {
	ctx := r.Context()
	query, err := handler.parseOffersQuery(r)
	if err != nil {
		return nil, err
	}

	offers, err := getOffersPage(ctx, handler.HistoryQ, query)
	if err != nil {
		return nil, err
	}

	return offers, nil
}

func getOffersPage(ctx context.Context, historyQ *history.Q, query history.OffersQuery) ([]hal.Pageable, error) {
	records, err := historyQ.GetOffers(query)
	if err != nil {
		return nil, err
	}

	ledgerCache := history.LedgerCache{}
	for _, record := range records {
		ledgerCache.Queue(int32(record.LastModifiedLedger))
	}

	if err := ledgerCache.Load(historyQ); err != nil {
		return nil, errors.Wrap(err, "failed to load ledger batch")
	}

	var offers []hal.Pageable
	for _, record := range records {
		var offerResponse horizon.Offer

		ledger, found := ledgerCache.Records[int32(record.LastModifiedLedger)]
		ledgerPtr := &ledger
		if !found {
			ledgerPtr = nil
		}

		resourceadapter.PopulateHistoryOffer(ctx, &offerResponse, record, ledgerPtr)
		offers = append(offers, offerResponse)
	}

	return offers, nil
}
