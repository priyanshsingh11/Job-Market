import React from "react";

const POWERBI_URL =
  "https://app.powerbi.com/links/9HRBeSy2Nq?ctid=cb1013ae-fb33-4405-8464-8d4ebfd3679d&pbi_source=linkShare&bookmarkGuid=73baaad8-e6a0-4dc4-98f1-18d007c093c6";

export default function StartScreen({ onStartApp }) {
  return (
    <div className="start-screen cinematic">
      {/* top bar */}
      <div className="topbar">
        <nav className="nav-left">
          <a className="logo-mini">Job Market</a>
        </nav>

        
      </div>

      {/* Vertical branding */}
      <aside className="left-vertical">
        <div className="vertical-brand">JOB MARKET</div>

        <div className="credits">
          <div className="credits-title">ML POWERED</div>
          <div className="credits-body">
            Salary Forecasting Model<br />
            Skill Intelligence Engine
          </div>

          <div className="mini-meta">
            Real-time insights for India’s tech job market.
          </div>
        </div>
      </aside>

      {/* center hero */}
      <section className="hero-area">
        <div className="hero-image" />

        {/* right panel */}
        <div className="product-panel">
          <div className="product-meta">
            <div className="colour">
              Feature: <strong>ML Salary Prediction</strong>
            </div>
            <div className="id">Model v1.0</div>
          </div>

          <div className="price-row">
            <div className="price">Live</div>

            <div className="product-actions">
              <button className="btn-primary" onClick={onStartApp}>
                Start Predicting
              </button>

              <button
                className="btn-ghost"
                onClick={() => window.open(POWERBI_URL, "_blank", "noopener")}
              >
                Open Dashboard
              </button>
            </div>
          </div>
        </div>
      </section>

      {/* bottom buttons */}
      <div className="start-bottom">
        <div className="start-actions">
          <button className="start-button" onClick={onStartApp}>
            Start the App
          </button>

          <button
            className="start-button ghost"
            onClick={() => window.open(POWERBI_URL, "_blank", "noopener")}
          >
            PowerBI Analysis
          </button>
        </div>

        <div className="legal">© Job Market Intelligence 2025</div>
      </div>
    </div>
  );
}
