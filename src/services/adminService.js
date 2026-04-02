// src/services/adminService.js
// Admin-specific operations: approve/reject, plan management, user management

const User = require('../models/User');
const Plan = require('../models/Plan');
const Request = require('../models/Request');
const Offer = require('../models/Offer');
const logger = require('../utils/logger');

/**
 * Approve a pending access request
 */
const approveRequest = async (requestId, adminId, planId, options = {}) => {
  const session = options.session || null;
  const expectedStatuses = Array.isArray(options.expectedStatuses) && options.expectedStatuses.length
    ? options.expectedStatuses
    : ['pending'];
  const expectedProcessingToken = options.expectedProcessingToken || null;

  const query = {
    _id: requestId,
    status: { $in: expectedStatuses },
  };
  if (expectedProcessingToken) {
    query.processingToken = expectedProcessingToken;
  }

  const request = await Request.findOneAndUpdate(
    query,
    {
      status: 'approved',
      actionDate: new Date(),
      actionBy: adminId,
      selectedPlanId: planId,
      processingBy: null,
      processingAt: null,
      processingToken: null,
      processingError: null,
    },
    { new: true, ...(session ? { session } : {}) }
  ).populate('userId');

  if (!request) throw new Error('Request already processed or not found');
  logger.info(`Request ${requestId} approved by admin ${adminId}`);
  return request;
};

/**
 * Reject a pending access request
 */
const rejectRequest = async (requestId, adminId) => {
  const request = await Request.findByIdAndUpdate(
    requestId,
    { status: 'rejected', actionDate: new Date(), actionBy: adminId },
    { new: true }
  );

  // Allow user to re-request
  await User.findOneAndUpdate({ telegramId: request.telegramId }, { status: 'inactive' });

  logger.info(`Request ${requestId} rejected by admin ${adminId}`);
  return request;
};

/**
 * Promote user to admin
 */
const addAdmin = async (telegramId) => {
  const user = await User.findOneAndUpdate(
    { telegramId },
    { role: 'admin' },
    { new: true }
  );
  if (!user) throw new Error(`User ${telegramId} not found`);
  logger.info(`User ${telegramId} promoted to admin`);
  return user;
};

/**
 * Demote admin back to user
 */
const removeAdmin = async (telegramId) => {
  const user = await User.findOneAndUpdate(
    { telegramId, role: 'admin' }, // cannot remove superadmin this way
    { role: 'user' },
    { new: true }
  );
  if (!user) throw new Error(`Admin ${telegramId} not found`);
  logger.info(`Admin ${telegramId} demoted to user`);
  return user;
};

/**
 * Get all active plans
 */
const getActivePlans = async () => {
  return Plan.find({ isActive: true }).sort({ durationDays: 1 });
};

/**
 * Get all plans (including inactive)
 */
const getAllPlans = async () => {
  return Plan.find({}).sort({ durationDays: 1 });
};

/**
 * Create a new subscription plan
 */
const createPlan = async (data) => {
  const plan = await Plan.create(data);
  logger.info(`Plan created: ${plan.name} (${plan.durationDays} days)`);
  return plan;
};

/**
 * Update a plan
 */
const updatePlan = async (planId, updates) => {
  const plan = await Plan.findByIdAndUpdate(planId, updates, { new: true });
  if (!plan) throw new Error(`Plan ${planId} not found`);
  logger.info(`Plan updated: ${plan.name}`);
  return plan;
};

/**
 * Delete a plan
 */
const deletePlan = async (planId) => {
  await Plan.findByIdAndDelete(planId);
  logger.info(`Plan ${planId} deleted`);
};

/**
 * Get active offers (not expired)
 */
const getActiveOffers = async () => {
  return Offer.find({ isActive: true, validTill: { $gt: new Date() } }).sort({ createdAt: -1 });
};

/**
 * Create a new offer
 */
const createOffer = async (data) => {
  const offer = await Offer.create(data);
  logger.info(`Offer created: ${offer.title}`);
  return offer;
};

/**
 * Delete an offer
 */
const deleteOffer = async (offerId) => {
  await Offer.findByIdAndDelete(offerId);
  logger.info(`Offer ${offerId} deleted`);
};

module.exports = {
  approveRequest,
  rejectRequest,
  addAdmin,
  removeAdmin,
  getActivePlans,
  getAllPlans,
  createPlan,
  updatePlan,
  deletePlan,
  getActiveOffers,
  createOffer,
  deleteOffer,
};
