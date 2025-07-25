# Empire Legal Tracker Roadmap

**last_updated:** 2025-07-24

## ✅ LIVE NOW
- Core legal schema (`legal_cases`, `legal_case_bills`, `legal_case_history`, `legal_bill_allocations`)
- Replit frontend wiring (initial legal list + detail views)
- Excel import system via Azure Function
- Audit and error logging via doorloop_pipeline_audit

## 🟨 NEXT
- [ ] Add `deal_id UUID` to `legal_cases`
- [ ] Create Supabase View `v_deal_legal_summary`
- [ ] Field app legal referral table `field_app_legal_referrals`
- [ ] Azure Function to convert field referrals into `legal_case`
- [ ] Update UI to triage referrals

## ⏳ LATER
- Dropbox file sync (metadata → Supabase, optional full sync)
- Microsoft 365 Court Date Reminders (Teams, Outlook, SharePoint)
- AI auto-tagging, anomaly detection, smart status updates

