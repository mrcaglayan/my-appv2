export default function AuthLayout({ children }) {
  return (
    <div className="grid min-h-screen place-items-center bg-gradient-to-b from-slate-100 to-slate-200 p-4">
      <div className="w-full max-w-sm rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
        {children}
      </div>
    </div>
  );
}
